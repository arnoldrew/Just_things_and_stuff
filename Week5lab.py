import logging
import time
import boto3
from botocore.exceptions import ClientError

# Create a table (prompt for the table name). It table must: with a primary (partition) and a secondary (hash) keys - 10pts
# Have a primary (partition) and a secondary (hash) key, both of string data types
# Have one Global Secondary Index (it is recommended that you reverse the primary key and the secondary key)
def create_table(TABLE_NAME):

    try:
        db_client = boto3.client('dynamodb')
        db_client.create_table(
          AttributeDefinitions=[
             {
                  'AttributeName': 'pk',
                  'AttributeType': 'S',
              },
              {
                  'AttributeName': 'sk',
                  'AttributeType': 'S',
              }
          ],
          KeySchema=[
              {
                  'AttributeName': 'pk',
                  'KeyType': 'HASH',
              },
              {
                  'AttributeName': 'sk',
                  'KeyType': 'RANGE',
              }
          ],
          ProvisionedThroughput={
              'ReadCapacityUnits': 2,
              'WriteCapacityUnits': 2,
          },
          GlobalSecondaryIndexes=[
          {
                'IndexName': "Index1",
                'KeySchema': [
                    {
                        'AttributeName': 'sk',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'pk',
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 2
                }
            },
        ],
          TableName=TABLE_NAME,
        )
        return True
    except ClientError as e:
        logging.error(e)
        return False

# This enables the pause while the table is being created
def describe_table(TABLE_NAME):

    try:
        db_client = boto3.client('dynamodb')
        response = db_client.describe_table(TableName=TABLE_NAME)
        return response['Table']['TableStatus']
    except ClientError as e:
        logging.error(e)
        return None

# Put 3 items that represent your an item in your individual project - 5pts
# All 3 items should have the same primary (partition) key with different secondary (hash) keys
# Put 3 more items that represent your an item in your individual project - 5pts
# All 3 items should have a different primary (partition) key with the same secondary (hash) keys
def put_item(pk,sk,inventory,primarycolor,secondarycolor,price,TABLE_NAME):

    try:
      db_client = boto3.client('dynamodb')
      db_client.put_item(
        Item={
          'pk': {
            'S': pk,
          },
          'sk': {
            'S': sk,
          },
          'inventory': {
            'S': inventory,
          },
          'primarycolor': {
            'S': primarycolor,
          },
          'secondarycolor': {
            'S': secondarycolor,
          },
          'price': {
            'N': str(price), #Note: Even though a number, it is passed as a string
          },
        },
        ReturnConsumedCapacity='TOTAL',
        TableName=TABLE_NAME,
      )        
      return True
    except Exception as e:
        logging.error(e)
        return False

# Gets and item from the table (prompt for the keys)- 5pts
def get_item(pk,sk,TABLE_NAME):

    try:
      db_client = boto3.client('dynamodb')
      response = db_client.get_item(
        Key={
          'pk': {
            'S': pk,
          },
          'sk': {
              'S': sk,
          },      
        },
        TableName=TABLE_NAME,
      )
      return response["Item"]
    except ClientError as e:
        logging.error(e)
        return None

# Deletes an item from the table (prompt for the keys)- 5pts
def delete_item(TABLE_NAME, pk, sk):

    try:
        db_client = boto3.client('dynamodb')
        db_client.delete_item(
            Key={
          'pk': {
            'S': pk,
          },
          'sk': {
              'S': sk,
          },      
        },
        TableName= TABLE_NAME,
        )
        return True
    except ClientError as e:
        logging.error(e)
        return False

# Deletes a table (prompt for the table name ) - 5pts
def delete_table(TABLE_NAME):

    try:
        db_client = boto3.client('dynamodb')
        db_client.delete_table(TableName=TABLE_NAME)
        return True
    except ClientError as e:
        logging.error(e)
        return False

# This will list the user's choices and run whichever function they choose, then ask if they wish to run another function.
def main():
    choice = int
    tablename = "rodentia"
    onoff = "yes"
    success = bool
    primarykey = str
    secondarykey = str
    gotgot = str

    while onoff == "yes":
        print("Please select from the following choices:")
        print("1. Create table")
        print("2. Add first 3 items (3 black and white animals)")
        print("3. Add second 3 items (3 guinea pigs)")
        print("4. Get an item from the table")
        print("5. Delete an item from a table")
        print("6. Delete a table")
        choice = int(input("Make your selection (1-6): "))

        if choice == 1:
            tablename = input("what do you want to call the table? ")
            success = create_table(tablename)

            while True:
                results = describe_table(tablename)
                if results == 'ACTIVE':
                    break
                time.sleep(10)
            
            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

#def put_item(pk,sk,inventory,primarycolor,secondarycolor,price,TABLE_NAME):
        # The function will run 3 times for each of the next 2 choices, once for each item added.
        # These 3 items will have the same primary key
        elif choice == 2:
            success = put_item("100", "guineapig", "1", "black", "white", "19.99", tablename)
            if success == True:
                print("First item added!")
            else:
                print("Item was not added...")
            
            success = put_item("100", "mouse", "21", "black", "white", "4.99", tablename)
            if success == True:
                print("Second item added!")
            else:
                print("Item was not added...")

            success = put_item("100", "gerbil", "5", "black", "white", "9.99", tablename)
            if success == True:
                print("Third item added!")
            else:
                print("Item was not added...")
    
        # These 3 items will have the same secondary key
        elif choice == 3:
            success = put_item("100", "guineapig", "1", "black", "white", "19.99", tablename)
            if success == True:
                print("First item added!")
            else:
                print("Item was not added...")
            
            success = put_item("101", "guineapig", "2", "brown", "white", "19.99", tablename)
            if success == True:
                print("Second item added!")
            else:
                print("Item was not added...")

            success = put_item("102", "guineapig", "4", "white", "none", "29.99", tablename)
            if success == True:
                print("Third item added!")
            else:
                print("Item was not added...")

        #gets an item from the table
        elif choice == 4:
            primarykey = input("What is the primary key you want to search for? ")
            secondarykey = input("What is the secondary key you want to search for? ")
            
            gotgot = get_item(primarykey, secondarykey, tablename)
            print(gotgot)

        #prompts the user for the info on the item to delete and then deletes it
        elif choice == 5:
            primarykey = input("what is the primary key of the item you want to delete? ")
            secondarykey = input("what is the secondary key of the item you want to delete? ")
            
            success = delete_item(tablename, primarykey, secondarykey)
            
            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        #prompts the user for the info on the table to delete and then deletes it
        elif choice == 6:
            tablename = input("Which table do you want to delete?")
            success = delete_table(tablename)   

            if success == True:
                print("It worked!")
            else:
                print("It did not work...")

        else:
            print("That is not a valid choice")

        onoff = input("Do you wish to do something else (yes or no)? ")



main()