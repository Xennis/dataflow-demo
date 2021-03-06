from __future__ import absolute_import

import argparse
import json

from past.builtins import unicode

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import ReadFromText
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from pipeline.common import common


# pylint: disable=too-few-public-methods
class Field(object):

    Element = u'element'
    Error = u'error'


class ParseRecord(beam.DoFn):

    TAG_BROKEN_DATA = u'broken_data'

    def __init__(self):
        super(ParseRecord, self).__init__()
        self.broken_counter = Metrics.counter(self.__class__, u'failed')

    def process(self, element, *args, **kwargs):
        try:
            j = json.loads(element)

            name = j.get(u'name')
            if not name:
                raise ValueError(u'field name is missing')
            name_parts = name.split(u' ')
            if len(name_parts) < 2:
                raise ValueError(u'field name contains not a least two strings')

            age = j.get(u'age')
            if not age:
                raise ValueError(u'field age is missing')
            try:
                age = int(age)
            except ValueError:
                raise ValueError(u'field age is not a integer')

            yield {
                u'first_name': name_parts[0],
                u'last_name': name_parts[1:],
                u'age': age
            }
        except ValueError as e:
            yield pvalue.TaggedOutput(self.TAG_BROKEN_DATA, {
                Field.Element: element,
                Field.Error: e
            })
            self.broken_counter.inc()


class ConvertToRow(beam.DoFn):

    def process(self, element, *args, **kwargs):
        yield {
            Field.Element: unicode(element.get(Field.Element)),
            Field.Error: unicode(element.get(Field.Error))
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        u'--input',
        type=str,
        help=u'Input directory to process.',
        required=True)
    parser.add_argument(
        u'--broken_dataset',
        type=str,
        default=u'customer',
        help=u'BigQuery dataset to write tables to. Must already exist.'
    )
    parser.add_argument(
        u'--broken_table_name',
        default=u'broken',
        help=u'BigQuery table name.'
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        parse_result = (
            p
            | u'read' >> ReadFromText(known_args.input).with_output_types(str)
            | u'parse' >> beam.ParDo(ParseRecord()).with_outputs(ParseRecord.TAG_BROKEN_DATA, main=u'customers')
        )

        customers, _ = parse_result
        broken_customers = parse_result[ParseRecord.TAG_BROKEN_DATA]

        # pylint: disable=expression-not-assigned
        (
            broken_customers
            | u'broken/convert' >> beam.ParDo(ConvertToRow())
            | u'broken/save' >> beam.io.WriteToBigQuery(
                dataset=known_args.broken_dataset,
                table=known_args.broken_table_name,
                schema=u'{element}:STRING,{error}:STRING'.format(element=Field.Element, error=Field.Error))
        )

        # pylint: disable=expression-not-assigned
        (
            customers
            | u'debug' >> beam.ParDo(common.Debug())
        )
