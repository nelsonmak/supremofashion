import io
import json
import logging
import mysql.connector

from fdk import response

def creatConnection(c_host, c_port, c_user, c_password, c_database):
    conn = mysql.connector.connect(host=c_host,port=c_port,user=c_user,password=c_password,database=c_database)
    return conn;

def handler(ctx, data: io.BytesIO = None):

    try:
        cfg = ctx.Config();
        c_host = cfg["host"]
        c_port = int(cfg["port"])
        c_user = cfg["user"]
        c_password = cfg["password"]
        c_database = cfg["database"]
        body = json.loads(data.getvalue()) 
        cust_id = body.get("CUST_ID")
        conn = creatConnection(c_host, c_port, c_user, c_password, c_database)
        cur = conn.cursor()
        cur.execute('''SELECT p.CUST_ID as CUST_ID, IF(p.prediction=1, "50%%", "10%%") as DISCOUNT
            FROM ml_data.customer_churn_predictions p
                where p.CUST_ID = '%s'
                LIMIT 1;''' % (cust_id))
        rv = cur.fetchone() 
        if rv is None:
            message = {'status': 'The user with the cust_id '+ cust_id +' does not exist'}
            return response.Response(
                ctx, response_data=json.dumps(message),
                headers={"Content-Type": "application/json"}
            )
        result = {'CUST_ID': rv[0], 'DISCOUNT': rv[1]}
        cur.close()	 
    except Exception as e:
        logging.error('Exception: %s' % e)	
    conn.close()
    return response.Response(
        ctx, response_data=json.dumps(result),
        headers={"Content-Type": "application/json"}
    )