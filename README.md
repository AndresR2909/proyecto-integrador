# Proyecto-integrador
Proyecto integrador primer semestre maestría en ciencia de datos

## Arquitectura

![Arquitectura nube propuesta](Arquitectura_azure.jpg)


## Estructura del Proyecto

- [aplicacion](#aplicacion)
- [ingesta_datos](#ingesta_datos)
- [modelamiento](#modelamiento)
- [Procesamiento_datos_almacenamiento_y_recuperacion_texto](#Procesamiento_datos_almacenamiento_y_recuperacion_texto)

## [aplicacion](aplicacion)

Envio de resumenes a whatsapp [ir](aplicacion).

![Aplicacion](resumen_whatsapp.png)

Tablas de resumenes de videos [ver tabla resumen videos](data/VideoSummaryTable.csv) y resumenes generales de todos los videos por dia (Map reduce). [ver tabla resumen diario](data/GeneralSummaryTable.csv)


## [Ingesta_datos](ingesta_datos)

Ingesta de transcripciones y metadata de videos de canales de youtube (Cada 3 horas). [Ver dataset](https://huggingface.co/datasets/AndresR2909/youtube_transcriptions_ingest)

![Ingesta de Datos](ingesta_1.png)

![Ingesta de Datos_2](ingesta_2.png)


## [Modelamiento](modelamiento)

Exploracion de modelos de ML para hacer agrupacion de embeddings con el objetivo agregar metadata de topicos y filtar textos (chunks) no relevantes para el almacenamiento en base de datos vectorial. [ver dataset](https://huggingface.co/datasets/AndresR2909/youtube_transcripcions_embeddigns)

![Modelamiento](diagrama_resumenes_llm_cluster.jpg)

* LDA
* Reduccion dimensionalidad: PCA, TSNE, UMAP
* Clustering: Kmeans, Dbscan, Hdbscan


## [Procesamiento_datos_almacenamiento_y_recuperacion_texto](procesamiento_datos_almacenamiento_recuperacion_texto)

Procesamiento de documentos, generacion de chunks, embbedings y almacenamiento en base de datos vectorial. [ver dataset](https://huggingface.co/datasets/AndresR2909/youtube_transcripcions_vector_data_base)

![Procesamiento de Datos, Almacenamiento de Texto](proyecto_1_arquiectura_vdb.jpg)


Recuperacion de texto de base de datos vectorial para generacion de resumenes

![Recuperación de Texto](proyecto_1_arquiectura_vdb_recuperacion_documentos.jpg)


Base de datos vectorial: Pinecone

![Pinecone](pinecone_1.png)




