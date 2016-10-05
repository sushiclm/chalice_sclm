from chalice import Chalice
import boto3
from boto3.dynamodb.conditions import Key, Attr
import tweepy

auth = tweepy.OAuthHandler("3RKVdzVR3xJGPsxDUFkCzw", "OBsyTWTG4dZPkL3v3POg160qhdRuIyi7iJ382JGzOo")
auth.set_access_token("364328378-gnJXDwC2fPUWbIVQIs8c9b8hAqZN3xNGJcOnxhE", "MK7MoYR1VbO1k1ygojwuXFzkKWArIWGCh7DjMFqDg")
api = tweepy.API(auth)

dynamodb = boto3.resource('dynamodb')

app = Chalice(app_name='sushiconlasmejores')
app.debug = True

def scrap_tweets():
    search_results = api.search(q="%22sushi%20con%20las%20mejores%22%20OR%20sushiconlasmejores", count=100)
    table = dynamodb.Table('tweets')
    for tweet in search_results:
        table.put_item(
            Item={
                'id_str': tweet.id_str,
                'created_at': int(tweet.created_at.strftime("%Y%m%d")),
                'text': tweet.text
                }
            )
    return {"count": len(search_results)}

def date_query(table):
    start_date = int(app.current_request.query_params['start_date'])
    results = table.query(
        KeyConditionExpression=Key('created_at').eq(start_date)
        )
    return {"tweets": results}

def get_tweets():
    table = dynamodb.Table('tweets')
    if 'start_date' in app.current_request.query_params:
        return date_query(table)
    else:
        results = table.scan()
    return {"tweets": results}

#test with curl -X POST https://sjtnnl4j9l.execute-api.us-east-1.amazonaws.com/dev/tweets
@app.route('/tweets', methods=['POST', 'GET'])
def tweets():
    request = app.current_request
    if request.method == 'POST':
        return scrap_tweets()
    else:
        return get_tweets()
