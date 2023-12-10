
import pymongo
import urllib.parse
import os

twoDCounties = ['G0100210', 'G0100270', 'G0100390', 
                'G0100070', 'G0100310', 'G0100150', 
                'G0100170', 'G0100230', 'G0100410', 
                'G0100370', 'G0100290', 'G0100010']
threeDCounties = ['G0100050', 'G0100750', 'G0100490', 
                  'G0100430', 'G0100570', 'G0100610', 
                  'G0100510', 'G0100250', 'G0101230', 
                  'G0100830', 'G0101250', 'G0100670']

def main():
    db = getDbRef()
    twoDResults = []
    for county in twoDCounties:
        pipeline = getPipeline(county)
        result = db.epa_temperature_sites.aggregate(pipeline)
        twoDResults.append({
                'average': result['Average'],
                'dataAvailability': result['Data Availability']
            }
        )
    threeDResults = []
    for county in threeDCounties:
        pipeline = getPipeline(county)
        result = db.epa_temperature_sites.aggregate(pipeline)
        threeDResults.append({
                'average': result['Average'],
                'dataAvailability': result['Data Availability']
            }
        )
    print(twoDResults, threeDResults)


def getPipeline(county_id):
    return [
        { 
            '$match': { 
                    'county_gis': county_id 
                } 
            },
        {
            '$lookup': {
                'from': 'epa_temperature_measurements',
                'let': { 'siteMatch': '$MonitoringLocationIdentifier' },
                'pipeline': [
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$MonitoringLocationIdentifier', '$$siteMatch'
                                ]
                            }
                        }
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$eq': [
                                    '$measurement_name', 'temperature, water'
                                ]
                            }
                        }
                    },
                    {
                        '$match': {
                            'epoch_time': {
                                '$gte': 946684800000, # 01/01/2000
                                '$lte': 954507600000 # 03/31/2000
                            }
                        }
                    }
                ]
            },
            'as': 'measurementData'
        },
        {
            '$unwind': { '$measurementData' }
        },
        {
            '$project': {
                'county_gis': 1,
                'measuremet_value': 1
            }
        },
        {
            '$group': {
                '_id': '$county_gis',
                'values': {
                    '$push': 'measurement_value'
                }
            }
        },
        {
            '$project': {
                '_id': 0,
                'county_gis': 1,
                'Average': {
                    '$avg': '$values'
                },
                'Data Availability': {
                    '$size': '$values'
                }
            }
        }
    ]

def getDbRef():
    username = urllib.parse.quote_plus(os.environ.get('ROOT_MONGO_USER'))
    password = urllib.parse.quote_plus(os.environ.get('ROOT_MONGO_PASS'))
    mongo = pymongo.MongoClient(f'mongodb://{username}:{password}@lattice-238:27018/')
    return mongo['sustaindb']

if __name__ == '__main__':
    main()
