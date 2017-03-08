from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import time


def clone_time_range(source_client,
                     destination_client,
                     start,
                     end,
                     quiet=False,
                     except_dbs=None):
    source_databases = set(
        x['name'] for x in source_client.get_list_database())
    destination_databases = set(
        x['name'] for x in destination_client.get_list_database())
    source_databases.discard('_internal')
    destination_databases.discard('_internal')
    if except_dbs is not None:
        for db in except_dbs:
            source_databases.discard(db)
            destination_databases.discard(db)

    for db in source_databases:
        if db not in destination_databases:
            if not quiet:
                print('{} in source, but not in destination - skipping'.
                      format(db))
            continue
        if not quiet:
            print('Replicating {}'.format(db))
        source_client.switch_database(db)
        destination_client.switch_database(db)

        measurements = source_client.query('SHOW MEASUREMENTS')
        measurement_names = [x['name'] for x in measurements.get_points()]

        for measurement in measurement_names:
            if not quiet:
                print('> Measurement: {}', measurement)
            tags = source_client.query('SHOW TAG KEYS FROM ' + measurement)
            fields = source_client.query('SHOW FIELD KEYS FROM ' + measurement)

            tag_names = set(x['tagKey'] for x in tags.get_points())
            field_info = dict(
                (x['fieldKey'], x['fieldType']) for x in fields.get_points())
            point_query = 'SELECT * FROM {}'.format(measurement)
            if start is not None or end is not None:
                point_query += ' WHERE'
            if start is not None:
                start_string = str(time.mktime(start.timetuple()))
                point_query += ' timestamp > ' + start_string
            if start is not None and end is not None:
                point_query += ' AND'
            if end is not None:
                end_string = str(time.mktime(end.timetuple()))
                point_query += ' timestamp < ' + end_string
            point_query += ';'

            def transform_to_dict_format(points):
                cnt = 0
                for point in points:
                    cnt += 1
                    tags = []
                    fields = []
                    for key, value in point.items():
                        if value is None:
                            continue
                        if key == 'time':
                            timestamp = value
                        elif key in tag_names:
                            tags.append((key, value))
                        elif key in field_info:
                            if field_info[key] == 'string':
                                fields.append((key, value))
                            elif field_info[key] == 'integer':
                                fields.append((key, value))
                            elif field_info[key] == 'float':
                                fields.append((key, float(value)))
                            elif field_info[key] == 'boolean':
                                fields.append((key, value))
                            elif field_info[key] == 'timestamp':
                                fields.append((key, value))
                            else:
                                assert(False)
                        else:
                            print('{}={} not found'.format(key, value))
                            assert(False)
                    yield {
                        "measurement": measurement,
                        "time": timestamp,
                        "tags": dict(tags),
                        "fields": dict(fields)
                    }
                if not quiet:
                    print('Read {} points'.format(cnt))

            params = {
                'chunked': 1000
            }
            points = transform_to_dict_format(source_client.query(
                point_query, epoch='ns', params=params).get_points())
            success = destination_client.write_points(points,
                                                      time_precision='n',
                                                      batch_size=1000)
            if success:
                if not quiet:
                    print('Wrote points {} to destination')
            else:
                if not quiet:
                    print('FAILED TO WRITE POINTS - aborting')
                return False
    return True
