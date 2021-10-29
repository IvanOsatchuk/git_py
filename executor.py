def call_term(template, return_first_line=True, log_on_cbuild=True):
    print("Run command: {}".format(template))
    process = subprocess.Popen(
        shlex.split(template), stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    instance_output, err = process.communicate()
    if err:
        print(err)
    if log_on_cbuild:
        for line in instance_output.decode("UTF-8").splitlines():
            print(line)
    if return_first_line:
        print("Return only the first line")
        return instance_output.decode("UTF-8").splitlines()[0]
    return instance_output.decode("UTF-8").splitlines()


def get_auth_token():
    """
    Retorna o token de acesso a API do Cloud Fusion

    returns: token de acesso
    """
    return call_term("gcloud auth print-access-token", log_on_cbuild=False)


def call_cdf_api():
    
    access_token = get_auth_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }