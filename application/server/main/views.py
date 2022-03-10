import json
import os
import pandas
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from application.server.main.tasks import create_task_inpi, create_task_harvest_patstat, create_task_p01_p04_patstat, \
    create_task_p05_patstat, create_task_p05b_patstat, create_task_p06_indiv_patstat, create_task_p07_entp_patstat, \
    create_task_p08_part_final_patstat, create_task_geo, create_json_patent_scanr

default_timeout = 43200000

main_blueprint = Blueprint('main', __name__, )


@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/inpi', methods=['GET'])
def run_task_inpi():
    """
    Unizpping files INPI database
    """
    ad, name = create_task_inpi()
    ad_json = ad.to_json(orient="records")
    name_json = name.to_json(orient="records")
    response_object = {
        'status': 'success',
        'addresses': ad_json,
        'names': name_json
    }
    return jsonify(response_object), 202


@main_blueprint.route('/harvest', methods=['GET'])
def run_task_harvest():
    """
    Harvest and unzip PATSTAT data
    """
    liste, df = create_task_harvest_patstat()
    l_json = json.dumps(liste)
    response_object = {
        'status': 'success',
        'files_PATSTAT': l_json
    }
    return jsonify(response_object), 202


@main_blueprint.route('/p01p04', methods=['GET'])
def run_task_p01p04():
    """
    Processing PATSTAT data
    """
    create_task_p01_p04_patstat()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202

@main_blueprint.route('/p05', methods=['GET'])
def run_task_p05():
    """
    Processing PATSTAT data
    """
    create_task_p05_patstat()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202


@main_blueprint.route('/p05b', methods=['GET'])
def run_task_p05b():
    """
    Processing PATSTAT data
    """
    create_task_p05b_patstat()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202

@main_blueprint.route('/p06', methods=['GET'])
def run_task_p06():
    """
    Processing PATSTAT data
    """
    create_task_p06_indiv_patstat()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202

@main_blueprint.route('/p07', methods=['GET'])
def run_task_p07():
    """
    Processing PATSTAT data
    """
    create_task_p07_entp_patstat()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202


@main_blueprint.route('/p08', methods=['GET'])
def run_task_p08():
    """
    Processing PATSTAT data
    """
    create_task_p08_part_final_patstat()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202


@main_blueprint.route('/json', methods=['GET'])
def run_task_json():
    """
    Processing PATSTAT data
    """
    create_json_patent_scanr()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202


@main_blueprint.route('/geo', methods=['GET'])
def run_task_geo():
    """
    Geocoding PATSTAT data
    """
    create_task_geo()
    response_object = {
        'status': 'success'
    }
    return jsonify(response_object), 202
