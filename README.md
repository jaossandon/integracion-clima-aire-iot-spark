# MCDI502 · Sumativa 3 · Taller II: Integración de fuentes heterogéneas con Apache Spark

**Curso:** Gestión de Datos y Tecnologías Big Data (MCDI502)

**Grupo 6:** Fernanda Ovalle Román, Sebastián Cajales Cid, César Lorca Bacián, Jorge Álvarez Ossandón

## Descripción

Este repositorio contiene el notebook del Taller II, en el que se construye un flujo de integración de datos con Apache Spark (PySpark) a partir de cuatro fuentes heterogéneas. Cada fuente se carga en un DataFrame de 100 registros, se valida y transforma, y se integra en un DataFrame unificado, sobre el cual se ejecutan consultas con Spark SQL.

| Fuente | Tipo | Variables | DataFrame |
|---|---|---|---|
| OpenMeteo | API REST pública | Temperatura y humedad a 2 m, precipitación, velocidad y dirección del viento a 10 m, índice UV | `df_openmeteo` |
| AQICN | API REST con token | $\mathrm{PM_{2.5}}$, $\mathrm{PM_{10}}$, $\mathrm{O_3}$, $\mathrm{NO_2}$, $\mathrm{SO_2}$ (subíndices AQI) | `df_aqicn` |
| MongoDB Atlas | Base de datos documental (ESP32) | Altura, presión atmosférica, $\mathrm{CO}$ | `df_mongo_bpm` |
| MySQL | Base relacional mediante API REST en PHP (ESP32) | Temperatura interior y humedad | `df_mysql_dht` |

La ubicación de análisis es Santiago de Chile (latitud -33,4489; longitud -70,6693).

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `mcdi502_s3_grupo6.ipynb` | Notebook completo, con los resultados de la ejecución del 14 de noviembre de 2026 |
| `.gitignore` | Excluye el entorno virtual, los binarios de Hadoop y los archivos temporales |

## Estructura del notebook

1. **Configuración del entorno:** librerías, Java, componente Hadoop para Windows, `SparkSession` y `SparkContext` con los conectores de MongoDB y MySQL, y parámetros de conexión.
2. **Ingesta de datos:** creación de los cuatro DataFrames con esquemas explícitos (`StructType`).
3. **Preprocesamiento:** validación de rangos físicos y duplicados, columna `altura_metros` ($h_m = h_{ft} \times 0{,}3048$) y columna `condicion_clima`.
4. **Integración:** diagnóstico temporal y construcción de `df_unificado` con una estrategia híbrida.
5. **Consultas con Spark SQL:** vistas temporales y consultas C1 a C7.
6. **Conclusiones:** síntesis, tabla de decisiones técnicas y limitaciones.

## Estrategia de integración

El diagnóstico temporal mostró que las cuatro fuentes no comparten ninguna hora en común, por lo que un join temporal de las cuatro produciría un DataFrame vacío. Se adoptó una estrategia híbrida:

- **MongoDB y MySQL** registran las lecturas del mismo dispositivo ESP32, con un desfase horario constante de cinco horas en MySQL. Tras corregir el desfase, ambas fuentes se unen por el instante de lectura (tolerancia de 8 segundos), con 100 de 100 lecturas emparejadas.
- **OpenMeteo y AQICN** se incorporan mediante un índice cronológico (1 a 100), porque no tienen superposición temporal con la telemetría del ESP32.

El número de filas se valida después de cada join. `df_unificado` contiene 100 filas y 32 columnas, con prefijos por fuente (`om_`, `aq_`, `mg_`, `my_`).

## Requisitos

- Windows 10 u 11
- Python 3.11 (las versiones 3.12 o superiores provocan fallas en los procesos de PySpark en Windows)
- Java 17 (por ejemplo, Eclipse Temurin)
- Conexión a internet (las cuatro fuentes se consultan en línea)

## Ejecución

1. Clonar el repositorio y crear el entorno virtual:

   ```
   git clone https://github.com/jaossandon/mcdi502-s3-spark.git
   cd mcdi502-s3-spark
   py -3.11 -m venv .venv
   ```

2. Abrir el notebook en VS Code o Jupyter, seleccionar el kernel de `.venv` y ejecutar todas las celdas (Run All). La primera celda instala las librerías necesarias (`pyspark==4.0.1`, `requests`, `pymongo`, `mysql-connector-python`) y la sección 1.2 descarga automáticamente los componentes de Hadoop para Windows.

3. Si Java 17 está instalado en otra ruta, actualizar la variable `JAVA_HOME` en la celda de la sección 1.2.

## Consideraciones

- **Datos en tiempo real:** como las fuentes se consultan en el momento de la ejecución, los valores cambian en cada ejecución. Las interpretaciones del notebook corresponden a los resultados guardados de la ejecución del 14 de noviembre de 2026. La metodología, las transformaciones y las validaciones son reproducibles en cualquier ejecución.
- **AQICN:** durante el desarrollo del trabajo, la última medición disponible de las estaciones de Santiago correspondía al 9 de noviembre de 2026, debido a una interrupción en la sincronización entre la red SINCA y AQICN.
- **Token de AQICN:** el notebook incluye un token gratuito de solo lectura, para que pueda ejecutarse sin configuración adicional.
