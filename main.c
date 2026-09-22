#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>

static void dormir_ms(long ms) {
    struct timespec ts;
    ts.tv_sec  = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

#define MAX_LINEA   1024
#define MAX_MENSAJE 256
#define MAX_ACTIVIDADES 10000
#define CAP_INICIAL 64

typedef enum { PENDIENTE, CORRIENDO, DONE, FALLIDA } Estado;

typedef struct {
    char id[32];
    char nombre[64];
    long tiempo_ms;
    int num_deps;
    char (*deps)[32];      /* ids de dependencias, tal como vienen del archivo */
    int *deps_idx;          /* indices ya resueltos de cada dependencia (-1 = no existe) */
    int pendientes;         /* dependencias validas aun no terminadas (algoritmo de Kahn) */
    int *dependientes;      /* indices de actividades que dependen de esta */
    int num_dependientes;
    pid_t pid;
    Estado estado;
    int pipe_fd[2];
    char mensaje[MAX_MENSAJE];
} Actividad;

/* ---------- Globales para el manejo de SIGINT ---------- */
/* g_idx_activos[i] guarda el indice en acts[] de la actividad cuyo pid
 * esta en g_pids_activos[i], en la misma posicion. Sirve para, al volver
 * de waitpid, encontrar la actividad sin recorrer las n actividades: solo
 * se recorre el bloque de procesos activos (tamano <= K). */
static pid_t g_pids_activos[MAX_ACTIVIDADES];
static int   g_idx_activos[MAX_ACTIVIDADES];
static int g_num_pids_activos = 0;

/* Mascara con solo SIGINT: se usa para bloquear la señal durante las
 * secciones criticas donde se modifican g_pids_activos / g_idx_activos /
 * g_num_pids_activos (registrar un pid recien creado, o remover uno tras
 * waitpid). Si SIGINT llegara justo en medio de esa actualizacion, el
 * manejador podria recorrer los arreglos a medio escribir y, en el peor
 * caso, dejar un hijo recien hecho fork sin recibir SIGTERM (quedaria
 * corriendo como huerfano tras el _exit(130) del padre). Bloqueando la
 * señal durante esas pocas lineas, cualquier SIGINT que llegue ahi queda
 * pendiente y se entrega recien cuando los arreglos vuelven a un estado
 * consistente. */
static sigset_t g_sigint_set;

/* ---------- Prototipos ---------- */
static char *trim(char *s);
int parsear_plan(const char *ruta, Actividad **out, int *out_n);
int resolver_dependencias(Actividad *acts, int n);
void manejador_sigint(int sig);
int ejecutar_plan(Actividad *acts, int n, int K, int prob_fallo);
void liberar_actividades(Actividad *acts, int n);
static void bloquear_sigint(sigset_t *anterior);
static void desbloquear_sigint(const sigset_t *anterior);

/* ---------- bloqueo/desbloqueo de SIGINT para secciones criticas ---------- */
static void bloquear_sigint(sigset_t *anterior) {
    sigprocmask(SIG_BLOCK, &g_sigint_set, anterior);
}

static void desbloquear_sigint(const sigset_t *anterior) {
    sigprocmask(SIG_SETMASK, anterior, NULL);
}

/* ---------- utilidades ---------- */
static char *trim(char *s) {
    while (isspace((unsigned char)*s)) s++;
    if (*s == '\0') return s;
    char *fin = s + strlen(s) - 1;
    while (fin > s && isspace((unsigned char)*fin)) fin--;
    fin[1] = '\0';
    return s;
}

/* ---------- parseo de plan.txt ---------- */
int parsear_plan(const char *ruta, Actividad **out, int *out_n) {
    FILE *f = fopen(ruta, "r");
    if (!f) {
        perror("No se pudo abrir el archivo del plan");
        return -1;
    }

    Actividad *acts = NULL;
    int n = 0;
    int cap = 0;
    char linea[MAX_LINEA];

    srand((unsigned int) time(NULL));

    while (fgets(linea, sizeof(linea), f)) {
        char *l = trim(linea);
        if (l[0] == '\0') continue;

        char *campo_id     = strtok(l, ":");
        char *campo_nombre = strtok(NULL, ":");
        char *campo_tiempo = strtok(NULL, ":");
        char *campo_deps   = strtok(NULL, "\n");

        if (!campo_id || !campo_nombre) {
            fprintf(stderr, "Linea mal formada, se ignora: %s\n", linea);
            continue;
        }

        /* Capacidad duplicada (64, 128, 256, ...) en vez de +1 por linea:
         * asi el costo de hacer crecer el arreglo se amortiza a O(1) por
         * actividad en vez de O(n) por actividad, lo que importa mucho
         * con planes de hasta 10000 lineas (criterio 2.4). */
        if (n == cap) {
            int nueva_cap = (cap == 0) ? CAP_INICIAL : cap * 2;
            Actividad *tmp = realloc(acts, (size_t) nueva_cap * sizeof(Actividad));
            if (!tmp) {
                perror("realloc");
                free(acts);
                fclose(f);
                return -1;
            }
            acts = tmp;
            cap = nueva_cap;
        }

        Actividad *a = &acts[n];
        memset(a, 0, sizeof(*a));

        strncpy(a->id, trim(campo_id), sizeof(a->id) - 1);
        strncpy(a->nombre, trim(campo_nombre), sizeof(a->nombre) - 1);

        char *tiempo_trim = campo_tiempo ? trim(campo_tiempo) : "";
        if (tiempo_trim[0] == '\0') {
            a->tiempo_ms = 100 + rand() % (5000 - 100 + 1);
        } else {
            a->tiempo_ms = atol(tiempo_trim);
        }

        a->num_deps = 0;
        a->deps = NULL;
        if (campo_deps) {
            char *deps_trim = trim(campo_deps);
            if (deps_trim[0] == '[') deps_trim++;
            size_t len = strlen(deps_trim);
            if (len > 0 && deps_trim[len - 1] == ']') deps_trim[len - 1] = '\0';
            deps_trim = trim(deps_trim);

            if (deps_trim[0] != '\0') {
                size_t len2 = strlen(deps_trim) + 1;
                char *copia = malloc(len2);
                if (!copia) {
                    perror("malloc");
                    free(acts);
                    fclose(f);
                    return -1;
                }
                memcpy(copia, deps_trim, len2);

                char *tok = strtok(copia, ",");
                while (tok) {
                    char *dep_id = trim(tok);
                    if (dep_id[0] != '\0') {
                        char (*nuevo)[32] = realloc(a->deps, (size_t)(a->num_deps + 1) * sizeof(*nuevo));
                        if (!nuevo) {
                            perror("realloc deps");
                            free(copia);
                            free(acts);
                            fclose(f);
                            return -1;
                        }
                        a->deps = nuevo;
                        strncpy(a->deps[a->num_deps], dep_id, 31);
                        a->deps[a->num_deps][31] = '\0';
                        a->num_deps++;
                    }
                    tok = strtok(NULL, ",");
                }
                free(copia);
            }
        }

        a->deps_idx = NULL;
        a->pendientes = 0;
        a->dependientes = NULL;
        a->num_dependientes = 0;
        a->pid = -1;
        a->estado = PENDIENTE;
        a->mensaje[0] = '\0';

        n++;
    }

    fclose(f);

    /* Ajustar al tamano exacto: libera la memoria de mas que dejo el
     * crecimiento por duplicacion. Si el shrink fallara (no deberia),
     * seguimos con el bloque anterior, que sigue siendo valido. */
    if (n > 0 && n < cap) {
        Actividad *ajustado = realloc(acts, (size_t) n * sizeof(Actividad));
        if (ajustado) acts = ajustado;
    }

    *out = acts;
    *out_n = n;
    return 0;
}

/* ---------- resolucion de dependencias: de strings a indices ---------- */
typedef struct { const char *id; int idx; } IdIdx;

static int cmp_ididx(const void *a, const void *b) {
    const IdIdx *pa = a;
    const IdIdx *pb = b;
    return strcmp(pa->id, pb->id);
}

static int cmp_id_clave(const void *clave, const void *elem) {
    const char *k = clave;
    const IdIdx *e = elem;
    return strcmp(k, e->id);
}

/* Convierte, una sola vez, las dependencias (guardadas como texto) en
 * indices dentro de acts[], y arma para cada actividad la lista inversa
 * de "quien depende de mi" (dependientes). Con esto el planificador deja
 * de necesitar busqueda lineal por id en tiempo de ejecucion: usa un
 * arreglo ordenado + busqueda binaria (O(log n)) una sola vez aqui, y
 * despues todo el recorrido del DAG es O(n + cantidad de dependencias). */
int resolver_dependencias(Actividad *acts, int n) {
    if (n == 0) return 0;

    IdIdx *tabla = malloc((size_t) n * sizeof(IdIdx));
    if (!tabla) { perror("malloc tabla ids"); return -1; }
    for (int i = 0; i < n; i++) {
        tabla[i].id = acts[i].id;
        tabla[i].idx = i;
    }
    qsort(tabla, (size_t) n, sizeof(IdIdx), cmp_ididx);

    int *out_count = calloc((size_t) n, sizeof(int));
    if (!out_count) { perror("calloc"); free(tabla); return -1; }

    /* Pasada 1: resolver cada dependencia a un indice y contar cuantos
     * "dependientes" tendra cada actividad objetivo. */
    for (int i = 0; i < n; i++) {
        Actividad *a = &acts[i];
        a->pendientes = 0;
        if (a->num_deps == 0) continue;

        a->deps_idx = malloc((size_t) a->num_deps * sizeof(int));
        if (!a->deps_idx) { perror("malloc deps_idx"); free(tabla); free(out_count); return -1; }

        for (int d = 0; d < a->num_deps; d++) {
            IdIdx *hallado = bsearch(a->deps[d], tabla, (size_t) n, sizeof(IdIdx), cmp_id_clave);
            int j = hallado ? hallado->idx : -1;
            a->deps_idx[d] = j;
            if (j >= 0) {
                a->pendientes++;
                out_count[j]++;
            }
            /* j < 0: dependencia inexistente, se ignora (mismo criterio
             * que la version anterior). */
        }
    }
    free(tabla);

    /* Pasada 2: reservar el arreglo de dependientes con el tamano exacto
     * (ya lo sabemos por out_count) y llenarlo. Sin esto, tendriamos que
     * hacer realloc de a uno, otra vez O(n^2) en el peor caso. */
    for (int i = 0; i < n; i++) {
        if (out_count[i] > 0) {
            acts[i].dependientes = malloc((size_t) out_count[i] * sizeof(int));
            if (!acts[i].dependientes) { perror("malloc dependientes"); free(out_count); return -1; }
        } else {
            acts[i].dependientes = NULL;
        }
        acts[i].num_dependientes = 0; /* aqui se usa como cursor de llenado */
    }
    for (int i = 0; i < n; i++) {
        Actividad *a = &acts[i];
        for (int d = 0; d < a->num_deps; d++) {
            int j = a->deps_idx[d];
            if (j >= 0) {
                acts[j].dependientes[acts[j].num_dependientes++] = i;
            }
        }
    }

    free(out_count);
    return 0;
}

/* ---------- SIGINT: la "inspeccion de la Seremi" ---------- */
void manejador_sigint(int sig) {
    (void) sig;
    const char msg[] = "\nSIGINT recibido: abortando todas las actividades...\n";
    write(STDOUT_FILENO, msg, sizeof(msg) - 1);
    for (int i = 0; i < g_num_pids_activos; i++) {
        kill(g_pids_activos[i], SIGTERM);
    }
    _exit(130); /* 128 + SIGINT, convencion habitual */
}

/* Propaga FALLIDA en cascada a todos los dependientes (directos e
 * indirectos) de una actividad que fallo. Iterativo con una pila propia
 * (en vez de recursion) para no arriesgar un desborde de stack si el DAG
 * tiene una cadena muy larga (hasta 10000 actividades en fila es un caso
 * valido segun el enunciado). */
static void propagar_fallo(Actividad *acts, int idx, int *pila, int *restantes) {
    int tope = 0;
    pila[tope++] = idx;
    while (tope > 0) {
        int actual = pila[--tope];
        for (int k = 0; k < acts[actual].num_dependientes; k++) {
            int dep = acts[actual].dependientes[k];
            if (acts[dep].estado == PENDIENTE) {
                acts[dep].estado = FALLIDA;
                printf("[FALLO] %s no se ejecuta: dependencia fallida\n", acts[dep].id);
                (*restantes)--;
                pila[tope++] = dep;
            }
        }
    }
}

/* ---------- planificador principal ---------- */
int ejecutar_plan(Actividad *acts, int n, int K, int prob_fallo) {
    /* cola: actividades listas para correr (pendientes == 0), en orden de
     * llegada. Cada actividad entra aqui a lo mas una vez en toda la
     * ejecucion, asi que un arreglo de tamano n alcanza siempre. */
    int *cola = malloc((size_t) n * sizeof(int));
    int *pila_fallo = malloc((size_t) n * sizeof(int));
    if (!cola || !pila_fallo) {
        perror("malloc planificador");
        free(cola);
        free(pila_fallo);
        return -1;
    }
    int frente = 0, fondo = 0;
    int activos = 0;
    int restantes = n; /* actividades que aun no llegan a DONE ni FALLIDA */

    for (int i = 0; i < n; i++) {
        if (acts[i].pendientes == 0) cola[fondo++] = i;
    }

    while (restantes > 0) {

        /* 1. lanzar actividades listas mientras haya cupo (K) */
        while (activos < K && frente < fondo) {
            int i = cola[frente++];
            if (acts[i].estado != PENDIENTE) continue; /* resguardo defensivo */

            if (pipe(acts[i].pipe_fd) == -1) {
                perror("pipe");
                free(cola); free(pila_fallo);
                return -1;
            }

            pid_t pid = fork();
            if (pid < 0) {
                perror("fork");
                free(cola); free(pila_fallo);
                return -1;
            }

            if (pid == 0) {
                /* ---- proceso hijo: simula la actividad ---- */
                close(acts[i].pipe_fd[0]);

                /* Propagacion de insumo: como este fork ocurre solo despues
                 * de que todas las dependencias de acts[i] terminaron, el
                 * hijo hereda (via fork) el arreglo acts[] ya con los
                 * mensajes de esas dependencias escritos por el padre.
                 * deps_idx ya viene resuelto (resolver_dependencias), asi
                 * que aqui no hace falta ninguna busqueda: acceso directo. */
                char insumos[MAX_MENSAJE];
                insumos[0] = '\0';
                for (int d = 0; d < acts[i].num_deps; d++) {
                    int j = acts[i].deps_idx[d];
                    if (j >= 0 && acts[j].mensaje[0] != '\0') {
                        size_t len = strlen(insumos);
                        snprintf(insumos + len, sizeof(insumos) - len,
                                 "%s[%s]", (len > 0 ? " " : ""), acts[j].mensaje);
                    }
                }
                if (insumos[0] != '\0') {
                    char notif[MAX_MENSAJE + 64];
                    int m = snprintf(notif, sizeof(notif),
                                      "[INSUMO] %s recibe: %s\n", acts[i].id, insumos);
                    if (m > 0) write(STDOUT_FILENO, notif, (size_t) m);
                }

                dormir_ms(acts[i].tiempo_ms);

                /* Simulacion opcional de fallo interno, activada solo si se
                 * define la variable de entorno PROB_FALLO (0-100). Sin
                 * ella, prob_fallo llega en 0 y esta rama nunca se toma. */
                if (prob_fallo > 0) {
                    unsigned semilla = (unsigned) getpid() ^ (unsigned) time(NULL);
                    srand(semilla);
                    if ((rand() % 100) < prob_fallo) {
                        char err[MAX_MENSAJE];
                        int m = snprintf(err, sizeof(err),
                            "[SIMULACION] %s fallo intencionalmente (PROB_FALLO)\n",
                            acts[i].id);
                        if (m > 0) write(STDOUT_FILENO, err, (size_t) m);
                        close(acts[i].pipe_fd[1]);
                        _exit(1);
                    }
                }

                char buf[MAX_MENSAJE];
                snprintf(buf, sizeof(buf), "Actividad %s (%s) completada",
                         acts[i].id, acts[i].nombre);
                write(acts[i].pipe_fd[1], buf, strlen(buf) + 1);
                close(acts[i].pipe_fd[1]);
                _exit(0);
            }

            /* ---- proceso padre ---- */
            close(acts[i].pipe_fd[1]);
            acts[i].pid = pid;
            acts[i].estado = CORRIENDO;

            /* Seccion critica: SIGINT bloqueada mientras se registra el
             * pid recien creado en los arreglos globales (ver comentario
             * junto a g_sigint_set). */
            sigset_t sigint_previo;
            bloquear_sigint(&sigint_previo);
            g_pids_activos[g_num_pids_activos] = pid;
            g_idx_activos[g_num_pids_activos] = i;
            g_num_pids_activos++;
            desbloquear_sigint(&sigint_previo);

            activos++;
        }

        /* 2. esperar a que termine alguna actividad corriendo */
        if (activos > 0) {
            int status;
            pid_t hijo = waitpid(-1, &status, 0);
            if (hijo < 0) {
                perror("waitpid");
                free(cola); free(pila_fallo);
                return -1;
            }

            /* Encontrar el indice del hijo recorriendo solo los procesos
             * activos (a lo mas K), no las n actividades. Seccion critica:
             * SIGINT bloqueada mientras se busca y se remueve (swap con el
             * ultimo) el pid de los arreglos globales, por la misma razon
             * que al registrarlo. */
            int idx = -1, pos = -1;
            sigset_t sigint_previo2;
            bloquear_sigint(&sigint_previo2);
            for (int p = 0; p < g_num_pids_activos; p++) {
                if (g_pids_activos[p] == hijo) { pos = p; idx = g_idx_activos[p]; break; }
            }
            if (pos >= 0) {
                g_pids_activos[pos] = g_pids_activos[g_num_pids_activos - 1];
                g_idx_activos[pos]  = g_idx_activos[g_num_pids_activos - 1];
                g_num_pids_activos--;
            }
            desbloquear_sigint(&sigint_previo2);

            if (idx >= 0) {
                ssize_t leidos = read(acts[idx].pipe_fd[0], acts[idx].mensaje,
                                       sizeof(acts[idx].mensaje) - 1);
                if (leidos > 0) acts[idx].mensaje[leidos] = '\0';
                close(acts[idx].pipe_fd[0]);

                if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                    acts[idx].estado = DONE;
                    restantes--;
                    printf("[OK] %s\n", acts[idx].mensaje);

                    /* Avisar solo a los dependientes directos: bajarles el
                     * contador de pendientes, y si alguno llega a 0, ya
                     * puede entrar a la cola de listos. */
                    for (int k = 0; k < acts[idx].num_dependientes; k++) {
                        int dep = acts[idx].dependientes[k];
                        if (acts[dep].estado == PENDIENTE) {
                            acts[dep].pendientes--;
                            if (acts[dep].pendientes == 0) {
                                cola[fondo++] = dep;
                            }
                        }
                    }
                } else {
                    acts[idx].estado = FALLIDA;
                    restantes--;
                    printf("[FALLO] Actividad %s fallo, se aborta su rama\n", acts[idx].id);
                    propagar_fallo(acts, idx, pila_fallo, &restantes);
                }
            }

            activos--;

        } else {
            /* activos == 0 implica que tambien la cola esta vacia (si
             * hubiera algo listo y activos < K, la fase 1 lo habria
             * lanzado). Si aun quedan actividades por terminar, nadie
             * puede avanzar: hay un ciclo o una dependencia irresoluble. */
            fprintf(stderr, "Error: no se puede progresar (posible ciclo en el DAG "
                             "o dependencia irresoluble)\n");
            free(cola); free(pila_fallo);
            return -1;
        }
    }

    free(cola);
    free(pila_fallo);
    return 0;
}

/* ---------- limpieza ---------- */
void liberar_actividades(Actividad *acts, int n) {
    for (int i = 0; i < n; i++) {
        free(acts[i].deps);
        free(acts[i].deps_idx);
        free(acts[i].dependientes);
    }
    free(acts);
}

/* ---------- main ---------- */
int main(int argc, char *argv[]) {
    /* stdout se comparte entre el padre (que usa printf, con buffer) y los
     * hijos (que usan write() directo, sin buffer, para [INSUMO] y
     * [SIMULACION]). Si stdout queda con buffer completo (el default al
     * redirigir a un archivo), el buffer del padre puede quedar sin
     * vaciar mientras un hijo ya escribio directamente al mismo archivo:
     * el hijo avanza el puntero del kernel, y cuando el padre por fin
     * vacia su buffer, sus bytes (mas viejos) llegan despues, en la
     * posicion equivocada, cortando y mezclando lineas. Forzar buffer por
     * linea hace que cada printf(...\n) del padre se vacie al sistema de
     * inmediato, igual que los write() de los hijos, preservando el
     * orden real de ejecucion sin perder rendimiento perceptible. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (argc != 3) {
        fprintf(stderr, "Uso: %s plan.txt K\n", argv[0]);
        return 1;
    }

    const char *ruta_plan = argv[1];
    int K = atoi(argv[2]);
    if (K <= 0) {
        fprintf(stderr, "K debe ser un entero positivo\n");
        return 1;
    }

    if (K > MAX_ACTIVIDADES) {
        fprintf(stderr, "K no puede ser mayor a %d (limite maximo soportado)\n",
                MAX_ACTIVIDADES);
        return 1;
    }

    /* g_sigint_set se arma una sola vez aqui y se usa despues para
     * bloquear/desbloquear SIGINT en las secciones criticas de
     * ejecutar_plan (ver bloquear_sigint/desbloquear_sigint). */
    sigemptyset(&g_sigint_set);
    sigaddset(&g_sigint_set, SIGINT);

    struct sigaction sa;
    sa.sa_handler = manejador_sigint;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);

    Actividad *actividades = NULL;
    int n = 0;
    if (parsear_plan(ruta_plan, &actividades, &n) != 0 || n == 0) {
        fprintf(stderr, "Error al parsear el plan (o el plan esta vacio)\n");
        return 1;
    }

    if (n > MAX_ACTIVIDADES) {
        fprintf(stderr, "El plan tiene %d actividades, se supera el maximo "
                        "soportado (%d)\n", n, MAX_ACTIVIDADES);
        liberar_actividades(actividades, n);
        return 1;
    }

    if (resolver_dependencias(actividades, n) != 0) {
        fprintf(stderr, "Error al resolver dependencias del plan\n");
        liberar_actividades(actividades, n);
        return 1;
    }

    int prob_fallo = 0;
    const char *pf = getenv("PROB_FALLO");
    if (pf) {
        prob_fallo = atoi(pf);
        if (prob_fallo < 0) prob_fallo = 0;
        if (prob_fallo > 100) prob_fallo = 100;
    }

    int resultado = ejecutar_plan(actividades, n, K, prob_fallo);

    liberar_actividades(actividades, n);
    return resultado == 0 ? 0 : 1;
}