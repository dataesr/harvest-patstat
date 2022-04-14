import json
import os
import pandas
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from application.server.main.tasks import create_task_all

default_timeout = 43200000

main_blueprint = Blueprint('main', __name__, )

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/harvest_compute', methods=['POST'])
def run_task_all():
    """
    All processes for patents
    """
    args = request.get_json(force=True)

    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='patents', default_timeout=default_timeout)
        task = q.enqueue(create_task_all, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/clean', methods=['POST'])
def run_task_clean():
    """
    All processes for patents
    """
    args = request.get_json(force=True)

    os.system(f'rm -rf {DATA_PATH}/data_PATSTAT_Global_*')
    os.system(f'rm -rf {DATA_PATH}/index_documentation_scripts_PATSTAT*')
    os.system(f'rm -rf {DATA_PATH}/tls*')

