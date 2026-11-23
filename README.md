# Integración de fuentes heterogéneas con Apache Spark: clima, calidad del aire y sensores IoT

Flujo de integración de datos con Apache Spark (PySpark) a partir de cuatro fuentes de naturaleza distinta: dos API REST públicas, una base de datos documental en la nube y una base relacional expuesta mediante una API en PHP. Cada fuente se carga en un DataFrame de 100 registros con esquema explícito, se valida y transforma, y se integra en un DataFrame unificado sobre el cual se ejecutan consultas con Spark SQL.

**Autor:** Jorge Álvarez Ossandón

---

## Fuentes de datos

| Fuente | Tipo | Variables | DataFrame |
|---|---|---|---|
| OpenMeteo | API REST pública | Temperatura y humedad a 2 m, precipitación, velocidad y dirección del viento a 10 m, índice UV | `df_openmeteo` |
| AQICN | API REST con token | $\mathrm{PM_{2.5}}$, $\mathrm{PM_{10}}$, $\mathrm{O_3}$, $\mathrm{NO_2}$, $\mathrm{SO_2}$ (subíndices AQI) | `df_aqicn` |
| MongoDB Atlas | Base de datos documental (ESP32) | Altura, presión atmosférica, $\mathrm{CO}$ | `df_mongo_bpm` |
| MySQL | Base relacional mediante API REST en PHP (ESP32) | Temperatura interior y humedad | `df_mysql_dht` |

La ubicación de análisis es Santiago de Chile (latitud -33,4489; longitud -70,6693).

## Hallazgos principales

| Tema | Resultado |
|---|---|
| Integración | Las cuatro fuentes no comparten ninguna hora en común: un join temporal de las cuatro habría producido un DataFrame vacío. La estrategia se derivó del diagnóstico de los datos y no se fijó de antemano. |
| Telemetría IoT | MongoDB y MySQL registran el mismo dispositivo ESP32, con un desfase horario constante de cinco horas en MySQL. Tras corregirlo, ambas fuentes se unen por el instante de lectura (tolerancia de 8 segundos), con 100 de 100 lecturas emparejadas. |
| Validación | Se verificó el número de filas después de cada join, la unicidad de las claves y la ausencia de emparejamientos duplicados. `df_unificado` conserva 100 filas y 32 columnas, sin nulos introducidos por la integración. |
| Calidad de las fuentes | Dos de las seis estaciones de AQICN en Santiago informaban como "última medición" datos de 2021 y 2022; se aplicó un filtro de vigencia de 30 días para no mezclar mediciones de distintos años. |
| Resultados | La serie meteorológica muestra el ciclo diario típico de la primavera en Santiago, con relación inversa entre temperatura y humedad. La calidad del aire se ubica en el rango "bueno" del AQI, con el material particulado como principal contaminante. |

## Competencias y métodos

| Área | Métodos y herramientas |
|---|---|
| Ingesta | Consumo de API REST con `requests`, conector oficial de MongoDB para Spark (serie 10.x, Scala 2.13), pipeline de agregación con ordenamiento y límite en el servidor (*pushdown*), respaldo con `pymongo`. |
| DataFrames | Esquemas explícitos (`StructType`), validación de rangos físicos y duplicados, columnas calculadas con `when` / `otherwise`, conversión de unidades ($h_m = h_{ft} \times 0{,}3048$). |
| Integración | Diagnóstico temporal entre fuentes, join por instante más cercano con tolerancia, clave de índice cronológico, prefijos por fuente, validación de conteos tras cada join. |
| Spark SQL | Vistas temporales, siete consultas sobre fuentes individuales y sobre el DataFrame unificado, verificación cruzada API de DataFrames = SQL. |
| Entorno | PySpark 4.0.1, Java 17, Python 3.11 en Windows, componentes de Hadoop para Windows, DNS sobre HTTPS para resolver la URI de MongoDB en redes con DNS bloqueado, materialización con `localCheckpoint` ante fuentes en tiempo real. |

## Contenido del notebook

| Sección | Contenido |
|---|---|
| 1 | Configuración del entorno: librerías, Java, Hadoop para Windows, `SparkSession` con los conectores de MongoDB y MySQL, parámetros de conexión |
| 2 | Ingesta de datos: los cuatro DataFrames con esquemas explícitos |
| 3 | Preprocesamiento: validación de rangos físicos y duplicados, `altura_metros` y `condicion_clima` |
| 4 | Integración: diagnóstico temporal y construcción de `df_unificado` con estrategia híbrida |
| 5 | Consultas con Spark SQL (C1 a C7) |
| 6 | Conclusiones, decisiones técnicas y limitaciones |

## Estructura del repositorio

```
integracion_clima_aire_iot.ipynb   notebook completo, con los resultados de la ejecución del 14 de noviembre de 2026
.gitignore                         excluye el entorno virtual, los binarios de Hadoop y los archivos temporales
```

## Cómo reproducir

Requisitos: Windows 10 u 11, Python 3.11 (las versiones 3.12 o superiores provocan fallas en los procesos de PySpark en Windows), Java 17 y conexión a internet.

```
git clone https://github.com/jaossandon/integracion-clima-aire-iot-spark.git
cd integracion-clima-aire-iot-spark
py -3.11 -m venv .venv
```

Abrir el notebook en VS Code o Jupyter, seleccionar el kernel de `.venv` y ejecutar todas las celdas. La primera celda instala las librerías necesarias (`pyspark==4.0.1`, `requests`, `pymongo`, `mysql-connector-python`) y la sección 1.2 descarga automáticamente los componentes de Hadoop para Windows. Si Java 17 está instalado en otra ruta, actualizar `JAVA_HOME` en la sección 1.2.

## Consideraciones

- **Datos en tiempo real:** las fuentes se consultan en el momento de la ejecución, por lo que los valores cambian en cada ejecución. Las interpretaciones del notebook corresponden a los resultados guardados de la ejecución del 14 de noviembre de 2026; la metodología, las transformaciones y las validaciones son reproducibles en cualquier ejecución.
- **AQICN:** durante el período analizado, la última medición disponible de las estaciones de Santiago correspondía al 9 de noviembre de 2026, por una interrupción en la sincronización entre la red SINCA y AQICN.
- **Token de AQICN:** el notebook incluye un token gratuito de solo lectura, para que pueda ejecutarse sin configuración adicional.