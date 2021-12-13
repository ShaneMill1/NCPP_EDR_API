import json
from dicttoxml import dicttoxml
import yaml
import json

class OPENAPI(object):

    HTML_PAGE = '''<!DOCTYPE html>
    <html lang="en">
        <head>    
            <meta charset="utf-8">
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.21.0/swagger-ui.css">
            <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.21.0/swagger-ui-bundle.js"></script>
            <script>

                function render() {
                    api_url = window.location.href.split('?')[0] + '?f=application/json';
                    var ui = SwaggerUIBundle({
                        url:  api_url,
                        dom_id: '#swagger-ui',
                        presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIBundle.SwaggerUIStandalonePreset
                        ]
                    });
                }

            </script>
        </head>

        <body onload="render()">
            <div id="swagger-ui"></div>
        </body>
    </html>'''

    def __init__(self, server_url):
        with open('EDR/schemas/edr_get.json') as json_file:
            self.open_api = json.load(json_file)
            self.open_api['servers'][0]['url'] = server_url

    def get_json(self):
        return json.dumps(self.open_api)


    def get_yaml(self):
        return yaml.dump(self.open_api)


    def get_xml(self):
        return dicttoxml(self.open_api, attr_type=False)


    def get_html(self):
        return self.HTML_PAGE
