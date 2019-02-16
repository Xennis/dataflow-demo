from __future__ import absolute_import

import logging

from pipeline.customer import customer

if __name__ == u'__main__':
    logging.getLogger().setLevel(logging.INFO)
    customer.run()
