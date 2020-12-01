import json
import os
import boto3
import fire
import logging
from os import listdir, system, getpid
from os.path import isfile, join

fh = boto3.client('firehose')
run_level_keys = ['play_id','character_chosen','ascension_level','local_time','is_ascension_mode'
,'floor_reached', 'playtime','score','neow_cost','seed_source_timestamp','circlet_count'
,'special_seed','seed_played','is_trial','campfire_rested','gold','neow_bonus','is_prod'
,'is_daily','chose_seed','campfire_upgraded','win_rate','timestamp','build_version'
,'purchased_purges','victory','player_experience','is_beta','is_endless','killed_by']


def get_files(input_path):
    logging.info("Running on path {}".format(input_path))
    file_list = [join(input_path, f) for f in listdir(input_path) if isfile(join(input_path, f))]
    return sorted(file_list, reverse=True)


# Extracts relevant key from each run. If missing, it can be assumed that 
def generate_run_fact_table(event, key_list, run_file):
    fact_table = {'source_file': run_file}
    for key in key_list:
        try:
            fact_table[key] = event[key]
        except KeyError:
            fact_table[key] = ''
    return(json.dumps(fact_table) + '\n')


def process_file(run_file):
    logging.info("Doing file: {}".format(run_file))
    with open(run_file) as json_file:
        run_logs = json.load(json_file)

    records_to_send = {
        'Data': generate_run_fact_table(
            run_log['event'], 
            run_level_keys, run_file
        ) for run_log in run_logs
    }

    response = fh.put_record_batch(
        DeliveryStreamName='test_stream',
        Records = [records_to_send]
    )
    if (response['FailedPutCount'] > 0):
        raise ValueError("PutRecord failed internally. Response:\n\n{}".format(response))
    else:
        logging.info("Great success: {}".format(run_file))


def main(input_path, logfile):
    logging.basicConfig(filename=logfile, level=logging.INFO)
    file_list = get_files(input_path)
    for f in file_list:
        process_file(f)


if __name__ == "__main__":
    fire.Fire(main)