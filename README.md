# El Planificador Dieciochero

Simulador y planificador de actividades modeladas como un DAG (Grafo Acíclico
Dirigido), implementado con procesos (`fork`), tuberías (`pipes`) y señales
(`SIGINT`) en C. Sin hilos ni mecanismos de sincronización.

## Compilación

```bash
gcc -Wall -Wextra -std=c17 -o planificador main.c -lpthread
```

(El flag `-lpthread` se incluye porque lo exige la pauta, aunque el programa no usa hilos.)

## Uso

```bash
./planificador plan.txt K
```

- `plan.txt`: archivo de texto plano con las actividades.
- `K`: límite de concurrencia (máximo de procesos activos a la vez). Debe
  ser un entero positivo y no puede superar 10000 (ver "Límites y
  validaciones" más abajo).

### Formato de `plan.txt`

```
ID_Actividad : Nombre_Actividad : tiempo_ms : [Dependencia1, Dependencia2, ...]
```

Ejemplo:

```
1 : prender_carbon : 500 :
2 : comprar_carne : 1200 :
3 : comprar_pan : 300 :
4 : asar_longaniza : 800 : 1, 2
5 : armar_choripan : 250 : 3, 4
6 : servir_mesa : 100 : 5
```

Si el campo `tiempo_ms` viene vacío, se asigna un valor aleatorio entre 100 y
5000 ms.

### Variable de entorno opcional: `PROB_FALLO`

Para probar el aislamiento de errores, se puede simular que cada actividad
falle con cierta probabilidad (0-100):

```bash
PROB_FALLO=30 ./planificador plan.txt 4
```

Sin definir `PROB_FALLO`, esta rama de código nunca se activa y el
comportamiento es el normal.

### Interrumpir la ejecución (Ctrl+C)

Al recibir `SIGINT`, el planificador envía `SIGTERM` a todos los procesos
activos y termina con código de salida 130.

## Funciones principales

- **`parsear_plan`**: lee `plan.txt` línea por línea, extrae ID, nombre,
  tiempo y dependencias, y construye el arreglo de `Actividad`. Usa
  crecimiento del arreglo por duplicación de capacidad (64, 128, 256, ...)
  en vez de `realloc` de a una actividad, para que el costo se amortice en
  O(1) por actividad — relevante para el criterio de carga de hasta 10000
  actividades.

- **`resolver_dependencias`**: convierte, una sola vez, las dependencias
  (guardadas como texto) a índices dentro del arreglo de actividades, usando
  una tabla ordenada + búsqueda binaria (O(log n)). De paso arma para cada
  actividad la lista inversa de "quién depende de mí" (`dependientes`), para
  no tener que recorrer todo el DAG cada vez que una actividad termina.

- **`ejecutar_plan`**: el planificador principal. Mantiene una cola de
  actividades listas (dependencias en 0) y, en cada iteración:
  1. Lanza actividades listas mientras haya cupo (`activos < K`).
  2. Espera de forma bloqueante (`waitpid(-1, ...)`, sin busy-waiting) a que
     termine alguna actividad activa, lee su mensaje del pipe, y si terminó
     bien, decrementa el contador de dependencias de sus dependientes
     directos (si algún dependiente llega a 0, entra a la cola). Si falló,
     propaga la falla a toda su rama del DAG (`propagar_fallo`).

- **`propagar_fallo`**: marca como `FALLIDA`, de forma iterativa (con una
  pila propia, no recursión, para no arriesgar desborde de stack con DAGs
  muy largos), a todos los dependientes directos e indirectos de una
  actividad que falló.

- **`manejador_sigint`**: handler de `SIGINT` que usa solo funciones
  async-signal-safe (`write`, `kill`) para enviar `SIGTERM` a todos los
  procesos activos y terminar con `_exit(130)`.

- **`bloquear_sigint` / `desbloquear_sigint`**: envuelven `sigprocmask` para
  bloquear/desbloquear `SIGINT` alrededor de las dos secciones críticas
  donde se modifican los arreglos globales de procesos activos (ver
  siguiente sección).

## Decisiones de diseño

- **Concurrencia con `fork` + `waitpid`, sin hilos.** El límite `K` se
  respeta llevando un contador `activos` que solo permite lanzar una nueva
  actividad si `activos < K`; la espera de que se libere un cupo es un
  `waitpid` bloqueante (nunca un `while` consultando en loop), por lo que no
  hay busy-waiting.

- **Paso de mensajes por pipes, sin condición de carrera en el orden.** Cada
  actividad tiene su propio `pipe`. El padre lee el mensaje de una actividad
  recién terminada (fase de `waitpid`) *antes* de que cualquiera de sus
  dependientes pueda hacer `fork`, porque una actividad solo entra a la cola
  de listos cuando su contador de dependencias pendientes llega a 0, lo cual
  ocurre justo después de leer ese mensaje. Así, el hijo dependiente hereda
  (vía `fork`) el arreglo `acts[]` ya con el mensaje de sus dependencias
  escrito, sin necesitar ningún mecanismo adicional de sincronización.

- **Bloqueo de `SIGINT` en las secciones críticas (evitar race conditions).**
  El manejador de `SIGINT` recorre los arreglos globales
  `g_pids_activos` / `g_idx_activos` para saber a quién mandarle `SIGTERM`.
  Esos arreglos se modifican en dos puntos del planificador: al registrar un
  proceso recién creado (`fork`) y al removerlo tras un `waitpid` (con
  swap-and-pop). Si `SIGINT` llegara justo en medio de esa actualización, el
  manejador podría leer los arreglos a medio escribir y, en el peor caso,
  dejar un hijo recién creado sin recibir `SIGTERM` (quedaría corriendo como
  huérfano). Por eso, `SIGINT` se bloquea con `sigprocmask` durante esas
  pocas líneas: cualquier señal que llegue ahí queda pendiente y se entrega
  recién cuando los arreglos vuelven a un estado consistente. El resto del
  tiempo (incluyendo mientras se espera en `waitpid`) la señal permanece
  desbloqueada, así que Ctrl+C sigue siendo responsive.

- **Validación de `K` y del tamaño del plan contra `MAX_ACTIVIDADES`.** Los
  arreglos `g_pids_activos` / `g_idx_activos` (usados por el manejador de
  `SIGINT`) tienen tamaño fijo `MAX_ACTIVIDADES = 10000`, acorde al máximo de
  actividades que pide soportar el enunciado. Si se ejecutara con `K` o un
  plan (`n`) mayor a ese tope, podría producirse un desborde de esos
  arreglos. Por eso `main` valida ambos casos al inicio y termina con un
  mensaje de error claro en vez de arriesgar un desborde de memoria.

- **Aislamiento de errores sin cerrar el programa.** Si una actividad
  termina con código de salida distinto de 0, o es terminada por una señal
  (`WIFSIGNALED`), se marca `FALLIDA` y se propaga la falla solo a su rama
  del DAG; el resto del plan sigue ejecutándose con normalidad.

- **Separación de módulos.** El código se organiza en funciones con
  responsabilidad única (parseo, resolución de dependencias, planificación,
  manejo de señales, limpieza) para que sea legible y fácil de revisar,
  evitando archivos `.h` adicionales innecesarios para un programa de este
  tamaño.