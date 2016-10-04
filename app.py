from chalice import Chalice
import boto3
import tweepy

auth = tweepy.OAuthHandler("3RKVdzVR3xJGPsxDUFkCzw", "OBsyTWTG4dZPkL3v3POg160qhdRuIyi7iJ382JGzOo")
auth.set_access_token("364328378-gnJXDwC2fPUWbIVQIs8c9b8hAqZN3xNGJcOnxhE", "MK7MoYR1VbO1k1ygojwuXFzkKWArIWGCh7DjMFqDg")
api = tweepy.API(auth)

dynamodb = boto3.resource('dynamodb')

app = Chalice(app_name='sushiconlasmejores')
app.debug = True


@app.route('/')
def index():
    return {'hello': 'world'}

#test with curl -X POST https://sjtnnl4j9l.execute-api.us-east-1.amazonaws.com/dev/tweets
@app.route('/tweets', methods=['POST'])
def scrap_tweets():
    search_results = api.search(q="hello", count=100)
    table = dynamodb.Table('tweets')
    for tweet in search_results:
        table.put_item(
            Item={
                'id_str': tweet.id_str,
                'created_at': str(tweet.created_at.strftime("%s")),
                'text': tweet.text
            }
            )
    return {"count": len(search_results)}
