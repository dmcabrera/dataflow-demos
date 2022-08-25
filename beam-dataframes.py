# Los dataframes, sirven para poder manipular
# colleciones de datos ocupando la popular
# API de Pandas, pero manteniendo la capacidad de
# procesamiento distribuido de Apache Beam

# Importo los paquetes
import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.convert import to_pcollection

# Demo que crea un Dataframe a partir de un archivo CSV
def demo_dataframe1():
    with beam.Pipeline() as pipeline:

        productos = pipeline | read_csv('productos.csv')

        def to_json(p):
            return {
                "Descripcion": p.Description,
                "Grupo": p.Group
            }

        productos_pc = (
            to_pcollection(productos)
            | 'Lo convierto a json' >> beam.Map(to_json)
            | 'Imprimo el resultado final' >> beam.Map(print)
        )


demo_dataframe1()

