import psycopg2

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS client(
        id SERIAL PRIMARY KEY,
        name VARCHAR(40) UNIQUE,
        last_name VARCHAR(40) UNIQUE,
        email VARCHAR(255) UNIQUE
        );
        """)
        
        cur.execute("""
        CREATE TABLE IF NOT EXISTS phone_number(
        id SERIAL PRIMARY KEY,
        client_id INTEGER REFERENCES client(id),
        phone_number VARCHAR(20) UNIQUE
        );
        """)
        conn.commit()

def add_client(conn, first_name, last_name, email, phones: list=None):
    
    if phones == None:
        sql_query_cl = "INSERT INTO client(name, last_name, email) VALUES(%s, %s, %s) RETURNING ID"
        cur = conn.cursor()
        cur.execute(sql_query_cl, (first_name, last_name, email))
        client_id = cur.fetchone()
        conn.commit()
        sql_query_num = "INSERT INTO phone_number(client_id, phone_number) VALUES (%s, NULL)"
        cur.execute(sql_query_num, client_id)
        conn.commit()
        cur.close()
        
        
    else:
        sql_query_cl = "INSERT INTO client(name, last_name, email) VALUES(%s, %s, %s) RETURNING ID"
        cur = conn.cursor()
        cur.execute(sql_query_cl, (first_name, last_name, email))
        client_id = cur.fetchone()
        conn.commit()
        for i in phones:
            phone_number = i
            sql_query_num = "INSERT INTO phone_number(client_id, phone_number) VALUES (%s, %s)"
            cur.execute(sql_query_num, (client_id, phone_number))
            conn.commit()
        cur.close()
        

def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phone_number(client_id, phone_number) VALUES(%s, %s)
        """, (client_id, phone))

def change_client(conn, client_id, first_name=None, last_name=None, email=None, changed_phones=None):
    with conn.cursor() as cur:
        update_query = "UPDATE client SET name = COALESCE(%s, name), last_name = COALESCE(%s, last_name), email = COALESCE(%s, email) WHERE id = %s"
        cur.execute(update_query, (first_name, last_name, email, client_id))
        
        if changed_phones:
            for old_phone, new_phone in changed_phones.items():
                cur.execute("UPDATE phone_number SET phone_number = %s WHERE client_id = %s AND phone_number = %s", (new_phone, client_id, old_phone))
        
        conn.commit()

def delete_phone(conn, client_id, phone: str):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM phone_number WHERE client_id = %s AND phone_number = %s", (client_id, phone))
        conn.commit()

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("SELECT phone_number FROM phone_number WHERE client_id = %s", (client_id,))
        phones = cur.fetchall()
        
        for phone in phones:
            delete_phone(conn, client_id, phone[0])
        
        cur.execute("DELETE FROM client WHERE id = %s", (client_id,))
        conn.commit()

def find_client(conn, first_name=None, last_name=None, email=None, phone: str=None):
    with conn.cursor() as cur:
        conditions = []
        parameters = []

        if first_name:
            conditions.append("c.name = %s")
            parameters.append(first_name)

        if last_name:
            conditions.append("c.last_name = %s")
            parameters.append(last_name)

        if email:
            conditions.append("c.email = %s")
            parameters.append(email)

        if phone:
            cur.execute("SELECT DISTINCT client_id FROM phone_number WHERE phone_number = %s", (phone,))
            phone_client_ids = [row[0] for row in cur.fetchall()]

            if phone_client_ids:
                conditions.append("c.id IN %s")
                parameters.append(tuple(phone_client_ids))
            else:
                return []

        if conditions:
            query = """
                SELECT c.*, pn.phone_number 
                FROM client c 
                LEFT JOIN phone_number pn ON c.id = pn.client_id 
                WHERE """ + " AND ".join(conditions)
            cur.execute(query, parameters)
            results = cur.fetchall()
            
            clients = {}
            for row in results:
                client_id = row[0]
                if client_id not in clients:
                    clients[client_id] = {
                        "id": row[0],
                        "name": row[1],
                        "last_name": row[2],
                        "email": row[3],
                        "phones": []
                    }
                if row[4]:
                    clients[client_id]["phones"].append(row[4])
        else:
            clients = {}

        return list(clients.values())



with psycopg2.connect(database="pysqlhw", user="postgres", password="postgres") as conn:
    create_db(conn)
    add_client(conn, "Иван", "Иванов", "ivanovich777@gmail.com", ["+79657749653", "+79650773883"])
    add_phone(conn, 1, '+79627134542')
    change_client(conn, 1, first_name="Андрей", last_name="Андреев", changed_phones={"+79657749653":"+71234567890"})
# В словаре первое значение это старый телефон, а второе - новый
    delete_phone(conn, 1, '+79650773883')
    # delete_client(conn, 1)
    print(find_client(conn, phone='+71234567890'))
    
conn.close()
