const {
  DynamoDBClient,
  PutItemCommand,
  QueryCommand,
  DeleteItemCommand,
  UpdateItemCommand,
  ScanCommand,
  GetItemCommand,
} = require('@aws-sdk/client-dynamodb');
const { marshall, unmarshall } = require('@aws-sdk/util-dynamodb');
const dbuex = require('dynamodb-update-expression');

const { chunk } = require('../chunk');

// eslint-disable-next-line no-promise-executor-return
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const createDynamodbClient = (tableName) => {
  const client = new DynamoDBClient();

  const getAll = async (items, { Command, params }) => {
    const { Items, LastEvaluatedKey } = await client.send(new Command(params));

    const mItems = Items.map((x) => unmarshall(x));

    const combinedItems = items.concat(mItems);

    if (LastEvaluatedKey) {
      return getAll(combinedItems, {
        Command,
        params: { ...params, ExclusiveStartKey: LastEvaluatedKey },
      });
    }

    return combinedItems;
  };

  return {
    insert: (input) => {
      const params = {
        TableName: tableName,
        Item: marshall(input),
      };

      return client.send(new PutItemCommand(params));
    },

    update: (params) => {
      const { Key, ExpressionAttributeValues } = params;

      const payload = {
        ...params,
        TableName: tableName,
        Key: marshall(Key),
        ExpressionAttributeValues: marshall(ExpressionAttributeValues),
      };

      return client.send(new UpdateItemCommand(payload));
    },

    batchUpdate: (data, size) => {
      const chunks = chunk(data, size);

      return chunks.reduce(async (promiseAcc, crr) => {
        const acc = await promiseAcc;

        const promises = crr.map((x) =>
          client.send(
            new UpdateItemCommand({
              ...x,
              TableName: tableName,
              Key: marshall(x.Key),
              ExpressionAttributeValues: marshall(x.ExpressionAttributeValues),
            })
          )
        );

        const resp = await Promise.all(promises);

        await sleep(1000);

        return acc.concat(resp);
      }, Promise.resolve([]));
    },

    put: ({ item }) => {
      const payload = {
        TableName: tableName,
        Item: marshall(item),
      };

      return client.send(new PutItemCommand(payload));
    },

    scan: async ({ filterExpression }) => {
      const params = {
        TableName: tableName,
        FilterExpression: filterExpression,
      };

      return getAll([], { Command: ScanCommand, params });
    },

    query: async (p) => {
      const { ExpressionAttributeValues } = p;

      const params = {
        ...p,
        TableName: tableName,
        ExpressionAttributeValues: marshall(ExpressionAttributeValues),
      };

      return getAll([], { Command: QueryCommand, params });
    },

    count: async ({
      keyCondition,
      filterExpression,
      expressionAttributeNames,
      expressionAttributeValues,
    }) => {
      const params = {
        TableName: tableName,
        KeyConditionExpression: keyCondition,
        FilterExpression: filterExpression,
        ExpressionAttributeNames: expressionAttributeNames,
        ExpressionAttributeValues: marshall(expressionAttributeValues),
        Select: 'COUNT',
      };

      const { Count } = await client.send(new QueryCommand(params));

      if (Count) {
        return Count;
      }
      return 0;
    },

    delete: async ({ keyCondition }) => {
      const params = {
        TableName: tableName,
        Key: marshall(keyCondition),
      };

      const resp = await client.send(new DeleteItemCommand(params));

      return resp;
    },

    get: async ({ keyCondition, attributesToGet }) => {
      const params = {
        TableName: tableName,
        Key: marshall(keyCondition),
        AttributesToGet: attributesToGet,
      };

      const { Item } = await client.send(new GetItemCommand(params));

      if (Item) {
        return unmarshall(Item);
      }
      return null;
    },
  };
};

module.exports = { createDynamodbClient, dbuex };
