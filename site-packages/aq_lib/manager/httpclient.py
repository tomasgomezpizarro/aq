from aq_lib.utils.logger import logger
import requests

class HttpClient:
    """
    Cliente HTTP. Se usa para que el "PublicAQHandlerLayer" se comunique con la api.

    :param _username: Nombre de usuario API.
    :param _password: Password de usuario API.
    :param _endpoint: Endpoint a donde van a ir las request de la api.
    """
    def __init__(self, _username, _password, _endpoint, _audience='arquants'):
        self.username = _username
        self.password = _password
        self.endpoint = _endpoint
        self.audience = _audience
        self.get_token()
        self.token = None

    def get_token(self):
        """
        Solicita el token y lo guarda.
        """
        headers = {'Content-Type': 'application/json', 'Authorization': 'my-auth-token'}
        payload = {'username': self.username, 'password': self.password, 'audience': self.audience}
        res = requests.post(self.endpoint + 'auth/login/', json=payload, headers=headers)
        if res.ok:
            self.token = res.json()['token']
            logger.info('(HTTP) Token OK.')
        else:
            print('Error on api get_token.')
            logger.info('(HTTP) Error on api get_token.')

    def get(self, _path, _payload=None):
        """
        Request GET a la API.
        :param _path:       Path de la api.
        :param _payload:    Datos.
        :return:
        """
        headers = {'Content-Type': 'application/json', 'Authorization': 'JWT %s' % self.token}
        res = requests.get(self.endpoint + _path, _payload, headers=headers)
        if res.ok:
            #
            # Manejo la response. Solamente es un confirmación de si la request ingreso ok a la api.
            #
            # return res
            print('response ok.')
            return res
        else:
            print('Error on GET %s, %s' % (self.endpoint + _path, _payload))

    def post(self, _path, event):
        """
        Request POST a la API.
        :param _path: Path de la api.
        :param event:    Evento arquants.
        :return response:
        """
        headers = {'Content-Type': 'application/json', 'Authorization': 'JWT %s' % self.token}
        res = requests.post(self.endpoint + _path, json=event, headers=headers)
        if res.ok:
            #
            # Manejo la response. Solamente es un confirmación de si la request ingreso ok a la api.
            #
            return res
        else:
            print('Error on GET %s, %s' % (self.endpoint + _path, event))


if __name__ == "__main__":
    pass
