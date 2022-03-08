import json
import os
import pandas
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from application.server.main.tasks import create_task_inpi, create_task_harvest_patstat, create_task_process_patstat, \
    create_task_geo, create_json_patent_scanr

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
    df_json = df.to_json(orient="records")
    response_object = {
        'status': 'success',
        'files_PATSTAT': l_json,
        'unzipped_files': df_json
    }
    return jsonify(response_object), 202


# @main_blueprint.route('/process', methods=['GET'])
# def run_task_process():
#     """
#     Processing PATSTAT data
#     """
#     pat, fam, fam_tech_codes, particip, role = create_task_process_patstat()
#     pat_json = pat.to_json(orient="records", lines=True)
#     fam_json = fam.to_json(orient="records", lines=True)
#     fam_tech_codes_json = fam_tech_codes.to_json(orient="records", lines=True)
#     particip_json = particip.to_json(orient="records", lines=True)
#     role_json = role.to_json(orient="records", lines=True)
#     response_object = {
#         'status': 'success',
#         'patents': pat_json,
#         'families': fam_json,
#         'family_technology_codes': fam_tech_codes_json,
#         'part': particip_json,
#         'role': role_json
#     }
#     return jsonify(response_object), 202



@main_blueprint.route('/json', methods=['GET'])
def run_task_json():
    """
    Processing PATSTAT data
    """
    patent_json_scanr = create_json_patent_scanr()
    return patent_json_scanr, 202


@main_blueprint.route('/geo', methods=['GET'])
def run_task_geo():
    """
    Geocoding PATSTAT data
    """
    best_scores_b, best_geocod = create_task_geo()
    best_scores_json = best_scores_b.to_json()
    best_geocod_json = best_geocod.to_json()
    response_object = {
        'status': 'success',
        'best_scores': best_scores_json,
        'best_geocoding': best_geocod_json
    }
    return jsonify(response_object), 202
