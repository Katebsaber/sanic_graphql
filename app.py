from sanic_graphql import GraphQLView
from sanic import Sanic
from graphql.execution.executors.asyncio import AsyncioExecutor

from schema import binance_1m_schema

app = Sanic(name="Sanic Graphql App")


@app.listener('before_server_start')
def init_graphql(app, loop):
    app.add_route(
        GraphQLView.as_view(schema=binance_1m_schema, batch=True, graphiql=True),
        '/candles',
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
