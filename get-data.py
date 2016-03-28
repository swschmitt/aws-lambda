#!/usr/bin/python

import boto3
import json
import time
import urllib2
import datetime
from itertools import cycle

API_URL = "https://api.steampowered.com/IDOTA2Match_570"
TIMEOUT = 290
EARLY_TIMEOUT = TIMEOUT - 0

def log(message):
    print datetime.datetime.now().isoformat() + ' | ' + str(message)

def lambda_handler(event={}, context={}):
    start_time = time.time()
    ex = None

    dynamodb = boto3.resource('dynamodb')
    data_table = dynamodb.Table('ad-data')
    metadata_table = dynamodb.Table('ad-metadata')
    response = metadata_table.get_item(
        Key={
            'role': 'latest_seq_num'
        }
    )
    latest_seq_num = response['Item']['match_seq_num']
    latest_date = response['Item'].get('date', "")
    latest_end_time = response['Item'].get('end_time', 0)

    response = metadata_table.get_item(
        Key={
            'role': 'dota_api_key'
        }
    )
    # dota_api_key = response['Item']['dota_api_key']
    dota_api_keys = response['Item']['dota_api_keys']
    key_iter = cycle(dota_api_keys)

    timediff=start_time-float(latest_end_time)
    log("Starting at seq num " + str(latest_seq_num) + ", behind " + str(datetime.timedelta(seconds=timediff)))

    total_matches = 0
    total_ad_matches = 0

    while 1:
        api_call_time = 0
        backoff = 1
        status = 0
        timed_out = False

        # Retry until success or timeout
        while status != 1:
            api_elapsed = time.time() - api_call_time

            # Exit if we've hit timeout
            if time.time() - start_time > TIMEOUT:
                timed_out = True
                break

            while api_elapsed < backoff:
                api_elapsed = time.time() - api_call_time
                time.sleep(1)

                # Exit if we've hit timeout
                if time.time() - start_time > TIMEOUT:
                    timed_out = True
                    break
            if timed_out:
                break

            for _ in range(len(dota_api_keys)):
                dota_api_key = next(key_iter)
                url = (API_URL + "/GetMatchHistoryBySequenceNum/V001/?key=" + dota_api_key +
                       "&start_at_match_seq_num=" + str(latest_seq_num) + "&matches_requested=1000")

                try:
                    doc = json.loads(urllib2.urlopen(url).read())
                    status = doc['result']['status']
                    if status == 1:
                        break
                except Exception:
                    # log("Try next API key at seq num " + str(latest_seq_num))
                    pass

            api_call_time = time.time()

            if status == 1:
                # log("GetMatchHistoryBySequenceNum success at seq num " + str(latest_seq_num))
                pass
            else:
                # log("GetMatchHistoryBySequenceNum failed at seq num " + str(latest_seq_num))
                if time.time() - start_time > EARLY_TIMEOUT:
                    # log("Early timeout")
                    timed_out = True
                    break

            backoff *= 2
            if backoff > 30:
                backoff = 30

        if timed_out:
            log("Time's up")
            break

        matches = doc['result']['matches']
        n_matches = len(matches)
        total_matches += n_matches

        if n_matches < 20:
            log("Less than 20 matches available")
            break

        for match in matches:
            # Filter out bad games
            if match['game_mode'] != 18:
                continue
            if match['human_players'] != 10:
                continue
            if match['start_time'] == 0:
                continue
            if match['duration'] < 900:
                continue
            leaver = False
            for player in match['players']:
                if player['leaver_status'] >= 2:
                    leaver = True
                    break
                # Remove additonal_units, it only causes problems
                player.pop('additional_units', None)
            if leaver:
                continue

            # Set date
            end_time = match['start_time'] + match['duration']
            date = datetime.date.fromtimestamp(end_time).isoformat()
            match['date'] = date

            # write match to dynamodb table
            try:
                response = data_table.put_item(Item=match)
                total_ad_matches += 1
            except Exception as e:
                log(match)
                ex = e
                break

        if ex:
            break

        latest_seq_num = matches[-1]['match_seq_num'] + 1
        # start_time is sometimes 0, skip the update if that is the case
        if matches[-1]['start_time'] > 0:
            latest_end_time = matches[-1]['start_time'] + matches[-1]['duration']

    # end while 1

    latest_date = datetime.date.fromtimestamp(latest_end_time).isoformat()

    # Update latest_seq_num
    metadata_table.put_item(
        Item={
            'role': 'latest_seq_num',
            'match_seq_num': latest_seq_num,
            'end_time': latest_end_time,
            'date': latest_date
        }
    )

    log("Finished at seq num " + str(latest_seq_num))
    log("Total/AD matches: " + str(total_matches) + "/" + str(total_ad_matches))

    if ex:
        raise ex

if __name__ == '__main__':
    lambda_handler()
