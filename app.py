from sanic_graphql import GraphQLView
from sanic import Sanic
import graphene

app = Sanic(name="Sanic Graphql App")


class Query(graphene.ObjectType):
    hello = graphene.String(description='A typical hello world')

    def resolve_hello(self, info):
        return 'World'


schema = graphene.Schema(query=Query)

app.add_route(
    GraphQLView.as_view(schema=schema, batch=True),
    '/name'
)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
