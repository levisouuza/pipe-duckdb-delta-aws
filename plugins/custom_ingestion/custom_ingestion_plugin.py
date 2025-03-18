from airflow.plugins_manager import AirflowPlugin
from processor.ingestion_processor import IngestionProcessor
from model.config_variables import ConfigVariables

# Definição do plugin
class CustomIngestionPlugin(AirflowPlugin):
    name = "custom_ingestion_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    flask_blueprints = []
    appbuilder_views = []
    appbuilder_menu_items = []
    global_operator_extra_links = []
    extra_links = []
    extra_modules = [IngestionProcessor, ConfigVariables]

# Esse arquivo permite que o Airflow reconheça os módulos personalizados automaticamente.
