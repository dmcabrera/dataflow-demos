# Job de ejemplo de Apache Beam, que ocupa el
# runner de Dataflow
import argparse
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# Defino el pipeline
def run(argv=None, save_main_session=True):

    # Parseo los argumentos de entrada
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=' + os.environ.get('RUNNER', 'DirectRunner'),
        '--project=' + os.environ.get('PROJECT', 'project-id'),
        '--staging_location=' + os.environ.get('STAGING_LOCATION', 'gs://bucket-name/stage/'),
        '--temp_location=' + os.environ.get('TEMP_LOCATION', 'gs://bucket-name/tmp/'),
        '--region=' + os.environ.get('REGION', 'us-central1'),
        '--job_name=simple-batch-job'])

    # Funcion que sirve para filtrar los clientes Chilenos
    def es_chileno(cliente):
        return cliente['nacionalidad'] == 'Chile'

    # Inicializo el pipeline
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as pipeline:
        clientes = (
            pipeline
            | 'Creo la lista de clientes' >> beam.Create([
                {
                    'rut': '112223334', 'nombre': 'Juan Gonzalez', 'nacionalidad': 'Chile'
                },
                {
                    'rut': '223334445', 'nombre': 'Rodrigo Martinez', 'nacionalidad': 'Chile'
                },
                {
                    'rut': '334445556', 'nombre': 'Paula Oviedo', 'nacionalidad': 'Chile'
                },
                {
                    'rut': '445556667', 'nombre': 'Elena Garcia', 'nacionalidad': 'Uruguay'
                },
                {
                    'rut': '556667778', 'nombre': 'Andrea Espinoza', 'nacionalidad': 'Colombia'
                },
            ])
            | 'Filtro los clientes chilenos' >> beam.Filter(lambda cliente: es_chileno(cliente))
            | beam.Map(print))


# Ejecuto el pipeline
if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()