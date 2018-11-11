import os
from flask import Flask
from config import config
from api.example import blueprint as example_blueprint

config_name = os.environ.get('ENV', 'development')
current_config = config[config_name]


def create_app():
    app = Flask(__name__)
    app.config.from_object(current_config)

    return app


app = create_app()
app.register_blueprint(example_blueprint)

if __name__ == "__main__":
    app.run(debug=True)
