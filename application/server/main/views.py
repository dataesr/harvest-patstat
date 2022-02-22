import json
import os
import pandas
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from application.server.main.tasks import create_task_inpi, create_task_harvest_patstat, create_task_process_patstat, \
    create_task_geo

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


@main_blueprint.route('/process', methods=['GET'])
def run_task_harvest():
    """
    Processing PATSTAT data
    """
    ptt_scope, ttles, abs, pub, pat, fam, fam_tech_codes, \
    part_init2, part_init3, part_ind, part_entp, \
    sex_table, siren_inpi_brevet, siren_inpi_generale, \
    part_entp_final, particip, participants, idext, role = create_task_process_patstat()
    ptt_scope_json = ptt_scope.to_json(orient="records")
    ttles_json = ttles.to_json(orient="records")
    abs_json = abs.to_json(orient="records")
    pub_json = pub.to_json(orient="records")
    pat_json = pat.to_json(orient="records")
    fam_json = fam.to_json(orient="records")
    fam_tech_codes_json = fam_tech_codes.to_json(orient="records")
    part_init2_json = part_init2.to_json(orient="records")
    part_init3_json = part_init3.to_json(orient="records")
    part_ind_json = part_ind.to_json(orient="records")
    part_entp_json = part_entp.to_json(orient="records")
    sex_table_json = sex_table.to_json(orient="records")
    siren_inpi_brevet_json = siren_inpi_brevet.to_json(orient="records")
    siren_inpi_generale_json = siren_inpi_generale.to_json(orient="records")
    part_entp_final_json = part_entp_final.to_json(orient="records")
    particip_json = particip.to_json(orient="records")
    participants_json = participants.to_json(orient="records")
    idext_json = idext.to_json(orient="records")
    role_json = role.to_json(orient="records")
    response_object = {
        'status': 'success',
        'patent_scope': ptt_scope_json,
        'titles': ttles_json,
        'abstracts': abs_json,
        'publications': pub_json,
        'patents': pat_json,
        'families': fam_json,
        'family_technology_codes': fam_tech_codes_json,
        'starting_file_participants': part_init2_json,
        'participants_start': part_init3_json,
        'individuals': part_ind_json,
        'companies': part_entp_json,
        'gender_individuals': sex_table_json,
        'siren_inpi_brevets': siren_inpi_brevet_json,
        'siren_inpi_general': siren_inpi_generale_json,
        'final_companies': part_entp_final_json,
        'final_participants': particip_json,
        'part': particip_json,
        'participants_end': participants_json,
        'idext': idext_json,
        'role': role_json
    }
    return jsonify(response_object), 202


@main_blueprint.route('/geo', methods=['GET'])
def run_task_harvest():
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
