# Ejemplos simples que sirven para entender los conceptos
# básicos de Apache Beam

import apache_beam as beam
from apache_beam.transforms.core import Map

def demo_filter1():
    with beam.Pipeline() as pipeline:
        nombres_con_s = (
            pipeline
            | 'Creo el arreglo de nombres inicial' >> beam.Create([
                    'Santiago',
                    'Martin',
                    'Andrea',
                    'Sofia',
                    'Joaquin'
                ])
            | 'Filtro los nombres con s' >> beam.Filter(lambda s: str.startswith(s, 'S'))
            | beam.Map(print)
        )

def es_chileno(cliente):
  return cliente['nacionalidad'] == 'Chile'

def demo_filter2():

    with beam.Pipeline() as pipeline:
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


def demo_filter3():
    with beam.Pipeline() as pipeline:

        nacionalidades = (
            pipeline | 'Nacionalidades por las que quiero filtrar' >> beam.Create([
            'Chile',
            'Uruguay',
        ]))

        clientes = (
            pipeline
            | 'Clientes' >> beam.Create([
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
            | 'Filtro nacionalidades' >> beam.Filter(
                lambda cliente,
                nacionalidades: cliente['nacionalidad'] in nacionalidades,
                nacionalidades=beam.pvalue.AsIter(nacionalidades))
            | beam.Map(print))


demo_filter1()