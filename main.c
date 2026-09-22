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

typedef enum { PENDIENTE, CORRIENDO, DONE, FALLIDA } Estado;

typedef struct {
    char id[32];
    char nombre[64];
    long tiempo_ms;
    int num_deps;
    char (*deps)[32];      /* arreglo dinamico de ids de dependencias */
    pid_t pid;
    Estado estado;
    int pipe_fd[2];
    char mensaje[MAX_MENSAJE];
} Actividad;

/* ---------- Globales para el manejo de SIGINT ---------- */
static pid_t g_pids_activos[MAX_ACTIVIDADES];
static int g_num_pids_activos = 0;

/* ---------- Prototipos ---------- */
static char *trim(char *s);
static int buscar_indice(Actividad *acts, int n, const char *id);
int parsear_plan(const char *ruta, Actividad **out, int *out_n);
int dependencias_cumplidas(Actividad *acts, int n, int idx);
int todas_terminadas(Actividad *acts, int n);
void manejador_sigint(int sig);
int ejecutar_plan(Actividad *acts, int n, int K, int prob_fallo);
void liberar_actividades(Actividad *acts, int n);

/* ---------- utilidades ---------- */
static char *trim(char *s) {
    while (isspace((unsigned char)*s)) s++;
    if (*s == '\0') return s;
    char *fin = s + strlen(s) - 1;
    while (fin > s && isspace((unsigned char)*fin)) fin--;
    fin[1] = '\0';
    return s;
}

static int buscar_indice(Actividad *acts, int n, const char *id) {
    for (int i = 0; i < n; i++) {
        if (strcmp(acts[i].id, id) == 0) return i;
    }
    return -1;
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

        Actividad *tmp = realloc(acts, (size_t)(n + 1) * sizeof(Actividad));
        if (!tmp) {
            perror("realloc");
            free(acts);
            fclose(f);
            return -1;
        }
        acts = tmp;

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

        a->pid = -1;
        a->estado = PENDIENTE;
        a->mensaje[0] = '\0';

        n++;
    }

    fclose(f);
    *out = acts;
    *out_n = n;
    return 0;
}

/* ---------- DAG ---------- */
/* devuelve: 0 = aun no listo, 1 = listo para correr, 2 = fallida (una dependencia fallo) */
int dependencias_cumplidas(Actividad *acts, int n, int idx) {
    Actividad *a = &acts[idx];
    for (int i = 0; i < a->num_deps; i++) {
        int j = buscar_indice(acts, n, a->deps[i]);
        if (j < 0) continue; /* dependencia inexistente: se ignora */
        if (acts[j].estado == FALLIDA) return 2;
        if (acts[j].estado != DONE) return 0;
    }
    return 1;
}

int todas_terminadas(Actividad *acts, int n) {
    for (int i = 0; i < n; i++) {
        if (acts[i].estado == PENDIENTE || acts[i].estado == CORRIENDO) return 0;
    }
    return 1;
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

/* ---------- planificador principal ---------- */
int ejecutar_plan(Actividad *acts, int n, int K, int prob_fallo) {
    int activos = 0;

    while (!todas_terminadas(acts, n)) {

        /* 1. lanzar actividades listas mientras haya cupo (K) */
        for (int i = 0; i < n && activos < K; i++) {
            if (acts[i].estado != PENDIENTE) continue;

            int listo = dependencias_cumplidas(acts, n, i);
            if (listo == 2) {
                acts[i].estado = FALLIDA;
                printf("[FALLO] %s no se ejecuta: dependencia fallida\n", acts[i].id);
                continue;
            }
            if (listo != 1) continue;

            if (pipe(acts[i].pipe_fd) == -1) {
                perror("pipe");
                return -1;
            }

            pid_t pid = fork();
            if (pid < 0) {
                perror("fork");
                return -1;
            }

            if (pid == 0) {
                /* ---- proceso hijo: simula la actividad ---- */
                close(acts[i].pipe_fd[0]);

                /* Propagacion de insumo: como este fork ocurre solo despues
                 * de que todas las dependencias de acts[i] terminaron, el
                 * hijo hereda (via fork) el arreglo acts[] ya con los
                 * mensajes de esas dependencias escritos por el padre. Aqui
                 * los leemos y los mostramos como "insumo recibido" antes
                 * de empezar a trabajar: esa es la propagacion del mensaje
                 * hacia la actividad dependiente que pide el enunciado. */
                char insumos[MAX_MENSAJE];
                insumos[0] = '\0';
                for (int d = 0; d < acts[i].num_deps; d++) {
                    int j = buscar_indice(acts, n, acts[i].deps[d]);
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
                 * ella, prob_fallo llega en 0 y esta rama nunca se toma:
                 * el comportamiento por defecto no cambia. Se usa para
                 * poder demostrar el aislamiento de errores (criterio 2.3)
                 * sin alterar la invocacion obligatoria ./planificador
                 * plan.txt K ni el formato de plan.txt. */
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
            g_pids_activos[g_num_pids_activos++] = pid;
            activos++;
        }

        /* 2. esperar a que termine alguna actividad corriendo */
        if (activos > 0) {
            int status;
            pid_t hijo = waitpid(-1, &status, 0);
            if (hijo < 0) {
                perror("waitpid");
                return -1;
            }

            int idx = -1;
            for (int i = 0; i < n; i++) {
                if (acts[i].pid == hijo) { idx = i; break; }
            }

            if (idx >= 0) {
                ssize_t leidos = read(acts[idx].pipe_fd[0], acts[idx].mensaje,
                                       sizeof(acts[idx].mensaje) - 1);
                if (leidos > 0) acts[idx].mensaje[leidos] = '\0';
                close(acts[idx].pipe_fd[0]);

                if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
                    acts[idx].estado = DONE;
                    printf("[OK] %s\n", acts[idx].mensaje);
                } else {
                    acts[idx].estado = FALLIDA;
                    printf("[FALLO] Actividad %s fallo, se aborta su rama\n", acts[idx].id);
                }
            }

            for (int i = 0; i < g_num_pids_activos; i++) {
                if (g_pids_activos[i] == hijo) {
                    g_pids_activos[i] = g_pids_activos[--g_num_pids_activos];
                    break;
                }
            }
            activos--;

        } else if (!todas_terminadas(acts, n)) {
            fprintf(stderr, "Error: no se puede progresar (posible ciclo en el DAG "
                             "o dependencia irresoluble)\n");
            return -1;
        }
    }

    return 0;
}

/* ---------- limpieza ---------- */
void liberar_actividades(Actividad *acts, int n) {
    for (int i = 0; i < n; i++) {
        free(acts[i].deps);
    }
    free(acts);
}

/* ---------- main ---------- */
int main(int argc, char *argv[]) {
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