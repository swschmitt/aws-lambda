#!/usr/bin/python

import boto3
import time
import datetime
from boto3.dynamodb.conditions import Key, Attr
import collections
import tempfile

TIMEOUT = 240

TABLES = {
    'single'  : { 'csv_header': 'ability'                  },
    'hero'    : { 'csv_header': 'hero,ability'             },
    'combo'   : { 'csv_header': 'ability1,ability2'        },
    'counter' : { 'csv_header': 'ability_win,ability_lose' },
    'synergy' : { 'csv_header': 'ability1,ability2'        },
    'item'    : { 'csv_header': 'ability,item'             },
}

def log(message):
    print datetime.datetime.now().isoformat() + ' | ' + str(message)

def write_csv(table, header, out_file):
    def stringify_key(key):
        if type(key) == tuple:
            return ','.join(map(str, key))
        else:
            return str(key)

    with open(out_file, 'w') as fd:
        fd.write(header + ',total,wins\n')
        for key in table['total']:
            fd.write(stringify_key(key) + ',' + str(table['total'][key]) + ',' + str(table['wins'][key]) + '\n')

def lambda_handler(event={}, context={}):
    start_time = time.time()
    timed_out = False;

    # Tables
    dynamodb = boto3.resource('dynamodb')
    data_table = dynamodb.Table('ad-data')
    metadata_table = dynamodb.Table('ad-metadata')

    # Get raw data info
    response = metadata_table.get_item(
        Key={
            'role': 'latest_seq_num'
        }
    )
    latest_seq_num = response['Item']['match_seq_num']
    latest_date = response['Item'].get('date')
    latest_date = datetime.datetime.strptime(latest_date, '%Y-%m-%d').date()
    latest_end_time = response['Item'].get('end_time', 0)

    # Get processed data info
    response = metadata_table.get_item(
        Key={
            'role': 'processed'
        }
    )
    processed_seq_num = response['Item'].get('match_seq_num', 0)
    process_seq_num = processed_seq_num
    processed_date = response['Item'].get('date')
    processed_date = datetime.datetime.strptime(processed_date, '%Y-%m-%d').date()
    processed_end_time = response['Item'].get('end_time', 0)
    processed_bucket = response['Item']['s3bucket']

    # Cancel if within 3 hours of data collection
    if latest_end_time - processed_end_time < 3*60*60:
        log("Data processing is caught up, exiting.")
        return

    log("Starting at " + str(processed_date) + " Timestamp " + str(processed_end_time))

    # Counters {total, wins}
    counters = dict()
    counters['single']  = {'total': collections.Counter(), 'wins': collections.Counter()}
    counters['hero']    = {'total': collections.Counter(), 'wins': collections.Counter()}
    counters['combo']   = {'total': collections.Counter(), 'wins': collections.Counter()}
    counters['counter'] = {'total': collections.Counter(), 'wins': collections.Counter()}
    counters['synergy'] = {'total': collections.Counter(), 'wins': collections.Counter()}
    counters['item']    = {'total': collections.Counter(), 'wins': collections.Counter()}

    def add_win(table, key):
        counters[table]['total'][key] += 1
        counters[table]['wins'][key] += 1

    def add_loss(table, key):
        counters[table]['total'][key] += 1

    while True:

        response = data_table.query(
            KeyConditionExpression=Key('date').eq(str(processed_date)) & \
                Key('match_seq_num').gt(process_seq_num)
        )

        if response['Count'] == 0:
            break

        for match in response['Items']:
            radiant_win = match["radiant_win"]
            abilities_win = set()
            abilities_lose = set()
            invalid = False

            for player in match["players"]:
                win = False
                abilities=set()
                hero = player["hero_id"]

                if "ability_upgrades" not in player:
                    invalid = True
                    break
                for upgrade in player["ability_upgrades"]:
                    ability = upgrade["ability"]
                    if ability != 5002:
                        abilities.add(ability)

                if player["player_slot"] < 5:
                    if radiant_win == True:
                        abilities_win |= abilities
                        win = True
                    else:
                        abilities_lose |= abilities
                else:
                    if radiant_win == True:
                        abilities_lose |= abilities
                    else:
                        win = True
                        abilities_win |= abilities

                # Make set of items
                items=set()
                for item in range(0,6):
                    item_i = player["item_%s" % item]
                    items.add(item_i)

                # Hero, Combo, Items
                if win:
                    for ability in abilities:
                        add_win('hero', (hero, ability))

                        for ability2 in abilities:
                            if ability < ability2:
                                add_win('combo', (ability, ability2))

                        for item in items:
                            add_win('item', (ability, item))

                if not win:
                    for ability in abilities:
                        add_loss('hero', (hero, ability))

                        for ability2 in abilities:
                            if ability < ability2:
                                add_loss('combo', (ability, ability2))

                        for item in items:
                            add_loss('item', (ability, item))

            # end for player in match

            if invalid:
                continue

            # Single, Counter, Synergy
            for ability in abilities_win:
                add_win('single', ability)

                for ability2 in abilities_lose:
                    if ability < ability2:
                        add_win('counter', (ability, ability2))
                    else:
                        add_loss('counter', (ability2, ability))

                for ability3 in abilities_win:
                    if ability < ability3:
                        add_win('synergy', (ability, ability3))

            for ability in abilities_lose:
                add_loss('single', ability)

                for ability2 in abilities_lose:
                    if ability < ability2:
                        add_loss('synergy', (ability, ability2))

            processed_end_time = match['start_time'] + match['duration']

        # end for match in response


        if 'LastEvaluatedKey' in response:
            log(str(response['Count']) + " : " + str(response['LastEvaluatedKey']))
            process_seq_num = response['LastEvaluatedKey']['match_seq_num']
        else:
            log(str(response['Count']))
            break

        # Timeout?
        if time.time() - start_time > TIMEOUT:
            timed_out = True
            break

    # end while True

    # Generate files, upload to S3
    temp_dir = tempfile.mkdtemp()
    s3_suffix = '.part' + str(processed_seq_num) + '-' + str(process_seq_num)
    s3_client = boto3.client('s3')
    for key in counters:
        s3_key = key + s3_suffix + '.csv'
        filename = temp_dir + '/' + s3_key
        # log('writing ' + filename)
        write_csv(counters[key], TABLES[key]['csv_header'], filename)

        s3_obj = str(processed_date) + '/' + s3_key
        log('uploading ' + s3_obj)
        s3_client.upload_file(filename, processed_bucket, s3_obj)

    # Completed the day
    if (not timed_out) and (latest_date != processed_date):
        processed_date += datetime.timedelta(days=1)
        process_seq_num = 0

    # Update latest_seq_num
    metadata_table.put_item(
        Item={
            'role': 'processed',
            'match_seq_num': process_seq_num,
            'end_time': processed_end_time,
            'date': str(processed_date),
            's3bucket': processed_bucket
        }
    )

if __name__ == '__main__':
    log('Enter')
    lambda_handler()
    log('Exit')
