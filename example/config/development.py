from .common import Config


class DevelopmentConfig(Config):
    DEBUG = True

    QUEUES = {
        'default': {
            "name": "queue-dlq",
        },
        'example': {
            'name': 'queue-example'
        }
    }
