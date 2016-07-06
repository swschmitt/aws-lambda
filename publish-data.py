#!/usr/bin/python

import boto3
import botocore
import json
import time
import datetime
from boto3.dynamodb.conditions import Key, Attr
import collections
import cPickle as pickle
import tempfile
import os
import errno

# This script has no timeout. Progress is incrementally saved in S3 such that
# if it were to be killed, the next call will pick up almost where it left off.

start_time = time.time()
ONE_DAY = datetime.timedelta(days=1)

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

def get_next_month(date):
    if date.month < 12:
        return datetime.date(year=date.year, month=date.month+1, day=1)
    else:
        return datetime.date(year=date.year+1, month=1, day=1)

def split_date_ranges(start_date, end_date):
    ''' list of tuples (start_date, type=('day' | 'month')) '''
    if start_date > end_date:
        raise Exception("start_date > end_date")

    if start_date == end_date:
        return [(start_date, 'day')]

    next_month = get_next_month(start_date)
    if (start_date.day == 1 and
        next_month - ONE_DAY <= end_date):
        if next_month - ONE_DAY == end_date:
            return [(start_date, 'month')]
        else:
            return [(start_date, 'month')] + split_date_ranges(next_month, end_date)
    else:
        return [(start_date, 'day')] + split_date_ranges(start_date + ONE_DAY, end_date)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def write_csv(table, out_file, header=None, full_header=None):
    def stringify_key(key):
        if type(key) == tuple:
            return ','.join(map(str, key))
        else:
            return str(key)

    assert(header or full_header)
    assert(not header or not full_header)

    with open(out_file, 'w') as fd:
        if header:
            fd.write(header + ',total,wins\n')
        else:
            fd.write(full_header)
        for key in table['total']:
            fd.write(stringify_key(key) + ',' + str(table['total'][key]) + ',' + str(table['wins'][key]) + '\n')

def merge_csv(in_files, out_file):
    table = dict()
    table = {'total': dict(), 'wins': dict()}

    for filename in in_files:
        first_header = None

        with open(filename, 'r') as fd:
            header = fd.readline()
            header_len = len(header.split(','))

            if first_header:
                assert(first_header == header)
            else:
                first_header = header

            for line in fd:
                if header_len == 3:
                    key, total, wins = line.split(',')
                if header_len == 4:
                    k1, k2, total, wins = line.split(',')
                    key = ','.join([k1, k2])

                if key in table['total']:
                    table['total'][key] += int(total)
                else:
                    table['total'][key] = int(total)

                if key in table['wins']:
                    table['wins'][key] += int(wins)
                else:
                    table['wins'][key] = int(wins)

    if out_file:
        # log('write_csv ' + out_file)
        write_csv(table, out_file, full_header=first_header)

def merge_day(table, date, bucket_name, temp_dir):
    date = str(date)
    path = temp_dir + '/' + bucket_name
    prefix = date + '/' + table + '.part'
    final_obj = date + '/' + table + '.csv'

    files = list()
    s3_client = boto3.client('s3')
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix)
    for content in response['Contents']:
        s3_obj = content['Key']
        filename = path + '/' + s3_obj
        # log('download ' + filename)
        s3_client.download_file(bucket_name, s3_obj, filename)
        files.append(filename)

    # Generate file
    filename = path + '/' + final_obj
    merge_csv(files, filename)

    # Upload to S3
    log('uploading ' + bucket_name + '/' + final_obj)
    s3_client.upload_file(filename, bucket_name, final_obj)

    # Delete partial files
    for content in response['Contents']:
        s3_client.delete_object(Bucket=bucket_name, Key=content['Key'])

def get_day(table, date, bucket_name, temp_dir):
    date = str(date)
    s3_key = date + '/' + table + '.csv'
    filename = temp_dir + '/' + bucket_name + '/' + s3_key

    if os.path.isfile(filename):
        return filename

    mkdir_p(os.path.dirname(filename))

    s3_client = boto3.client('s3')
    # log('get ' + s3_key)
    try:
        s3_client.download_file(bucket_name, s3_key, filename)
    except:
        # log('not found, generating ' + s3_key)
        merge_day(table, date, bucket_name, temp_dir)

    return filename

def merge_month(table, date, bucket_name, temp_dir):
    month = datetime.datetime.strftime(date, '%Y-%m')
    path = temp_dir + '/' + bucket_name
    final_obj = month + '/' + table + '.csv'

    files = list()
    assert(date.day == 1)
    end_date = get_next_month(date) - ONE_DAY
    current_date = date;
    while current_date <= end_date:
        filename = get_day(table, current_date, bucket_name, temp_dir)
        # log(filename)
        files.append(filename)

        current_date = current_date + ONE_DAY

    # Generate file
    filename = path + '/' + final_obj
    merge_csv(files, filename)

    # Upload to S3
    s3_client = boto3.client('s3')
    log('uploading ' + bucket_name + '/' + final_obj)
    s3_client.upload_file(filename, bucket_name, final_obj)

def get_month(table, date, bucket_name, temp_dir):
    month = datetime.datetime.strftime(date, '%Y-%m')
    s3_key = month + '/' + table + '.csv'
    filename = temp_dir + '/' + bucket_name + '/' + s3_key

    if os.path.isfile(filename):
        return filename

    mkdir_p(os.path.dirname(filename))

    s3_client = boto3.client('s3')
    # log('get ' + s3_key)
    try:
        s3_client.download_file(bucket_name, s3_key, filename)
    except:
        # log('not found, generating ' + s3_key)
        merge_month(table, date, bucket_name, temp_dir)

    return filename

def merge_data(table, start_date, end_date, bucket_name, out_file):
    timed_out = False;

    get_function = {
        'day': get_day,
        'month': get_month,
    }

    files = list()
    temp_dir = tempfile.mkdtemp()
    for date_range in split_date_ranges(start_date, end_date):
        # log('get ' + str(date_range[1]) + ': ' + str(date_range[0]))

        filename = get_function[date_range[1]](table, date_range[0], bucket_name, temp_dir)
        # log(filename)
        files.append(filename)

    merge_csv(files, out_file)

def publish_data(start_date, end_date, processed_bucket, publish_bucket, endpoint):
    s3_prefix = (
        'publish-' +
        datetime.datetime.strftime(start_date, '%Y%m%d') +
        '-' +
        datetime.datetime.strftime(end_date, '%Y%m%d') +
        '-'
    )

    index = dict()
    temp_dir = tempfile.mkdtemp()
    s3_client = boto3.client('s3')
    for table in TABLES:
        s3_key = s3_prefix + table + '.csv'
        filename = temp_dir + '/' + s3_key
        index[table] = 'http://' + endpoint + '/' + s3_key

        # log('get ' + s3_key)
        try:
            s3_client.download_file(publish_bucket, s3_key, filename)
        except:
            # log('not found, generating ' + s3_key)
            merge_data(table, start_date, end_date, processed_bucket, filename)

            log('uploading ' + publish_bucket + '/' + s3_key)
            s3_client.upload_file(filename, publish_bucket, s3_key)

    s3_key = 'index.json'
    index['timestamp'] = int(time.time())
    index['start_date'] = str(start_date)
    index['end_date'] = str(end_date)
    filename = temp_dir + '/' + s3_key
    with open(filename, 'w') as fd:
        json.dump(index, fd)

    log('uploading ' + publish_bucket + '/' + s3_key)
    s3_client.upload_file(filename, publish_bucket, s3_key)

def lambda_handler(event={}, context={}):
    # Tables
    dynamodb = boto3.resource('dynamodb')
    metadata_table = dynamodb.Table('ad-metadata')

    # Get publish info
    response = metadata_table.get_item(
        Key={
            'role': 'publish'
        }
    )
    publish_date = response['Item'].get('date', None)
    if publish_date:
        publish_date = datetime.datetime.strptime(publish_date, '%Y-%m-%d').date()
    start_date = response['Item'].get('start_date')
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    publish_bucket = response['Item']['s3bucket']
    endpoint = response['Item']['endpoint']

    # Get processed data info
    response = metadata_table.get_item(
        Key={
            'role': 'processed'
        }
    )
    processed_date = response['Item'].get('date')
    processed_date = datetime.datetime.strptime(processed_date, '%Y-%m-%d').date()
    processed_bucket = response['Item']['s3bucket']

    # Cancel if within a day of data processing
    if publish_date and (processed_date - publish_date).days <= 1:
        log("Publish is caught up, exiting.")
        return

    end_date = processed_date - ONE_DAY
    log("Starting publish " + str(start_date) + " to " + str(end_date))

    publish_data(start_date=start_date, end_date=end_date, processed_bucket=processed_bucket, publish_bucket=publish_bucket, endpoint=endpoint)

    metadata_table.update_item(
        Key={
            'role': 'publish',
        },
        UpdateExpression='SET #ea1=:ea1',
        ExpressionAttributeNames={
            '#ea1': 'date',
        },
        ExpressionAttributeValues={
            ':ea1': str(end_date),
        },
    )

if __name__ == '__main__':
    log('Enter')
    lambda_handler()
    log('Exit')
