from __future__ import absolute_import

import logging

import apache_beam as beam


class Debug(beam.DoFn):

    def process(self, element, *args, **kwargs):
        logging.debug(element)
        yield element
