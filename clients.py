import psycopg2


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS client(
                client_id SERIAL PRIMARY KEY,
                first_name VARCHAR(80) NOT NULL,
                last_name VARCHAR(80) NOT NULL, 
                email VARCHAR(80) UNIQUE NOT NULL       
            );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS telephone(
                client_id INTEGER NOT NULL REFERENCES client(client_id),
                phone BIGINT UNIQUE NOT NULL
            );
            """)
        conn.commit()
    return 

def add_client(conn, first_name, last_name, email, phones=None):
    with conn.cursor() as cur:        
        cur.execute("""
            INSERT INTO client(first_name, last_name, email) VALUES(%s, %s, %s) RETURNING client_id;
            """, (first_name, last_name, email,))
        client_id=cur.fetchone()[0]
        

    with conn.cursor() as cur:
        if phones!=None:
            tel=phones.split("/") 
            for i in range(len(tel)):
                cur.execute("""
                    INSERT INTO telephone(client_id, phone) VALUES(%s, %s);
                    """, (client_id, int(tel[i]),))
        conn.commit()
    return                   # print(f'Имя : {first_name},  Фамилия : {last_name}, email : {email}, телефон : {phones}')
            
def add_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO telephone(client_id, phone) VALUES(%s, %s);
            """, (client_id, phone,))
        conn.commit()
    return                  #  print(f'Телефон {phone} добавлен клиенту с id {client_id}')


def change_client(conn, client_id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        if first_name!=None:
            cur.execute("""
                UPDATE client SET first_name=%s WHERE client_id=%s;
                """, (first_name, client_id))
        if last_name!=None:
            cur.execute("""
                UPDATE client SET last_name=%s WHERE client_id=%s;
                """, (last_name, client_id))
        if email!=None:
            cur.execute("""
                UPDATE client SET email=%s WHERE client_id=%s;
                """, (email, client_id))
        if phones!=None:
            tel=phones.split("/") 
            for i in range(len(tel)):
                cur.execute("""
                    UPDATE telephone SET phone=%s WHERE client_id=%s;
                    """, (int(tel[i]), client_id))
        conn.commit()
    return   # print('Изменения сохранены') 
      

def delete_phone(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM telephone WHERE client_id=%s AND phone=%s;
        """, (client_id, phone,))
        conn.commit()   
    return     # print('Телефон удален') 

def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM telephone WHERE client_id=%s;
        """, (client_id,))
        conn.commit()
    with conn.cursor() as cur:
        cur.execute("""
        DELETE FROM client WHERE client_id=%s;
        """, (client_id,))
        conn.commit()   
    return  # print('Клиент удален') 

# Дополнительная функция
def phone_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT phone FROM telephone WHERE client_id=%s;
        """, (client_id,))
        tel=cur.fetchall()
        if tel==[]:
            print('нет', end='  ')
        else:
            for i in range(len(tel)):
                print(*tel[i], end='  ')
    return

# Дополнительная функция
def find_last_client(conn, first_name=None, last_name=None, id=None):
    with conn.cursor() as cur:
        if first_name!=None or last_name!=None:    
            if first_name!=None and last_name!=None:
                cur.execute("""
                    SELECT * FROM client WHERE first_name=%s AND last_name=%s AND client_id=%s;
                    """, (first_name, last_name, id,))
                cl=cur.fetchone()
            elif first_name!=None:
                cur.execute("""
                    SELECT * FROM client WHERE first_name=%s AND client_id=%s;
                    """, (first_name, id,))
                cl=cur.fetchone()
            elif last_name!=None:
                cur.execute("""
                    SELECT * FROM client WHERE last_name=%s AND client_id=%s;
                    """, (last_name, id,))
                cl=cur.fetchone()
            if cl==None:
                print('Клиент не найден')
            else:
                print(f'id : {cl[0]},  Имя : {cl[1]},  Фамилия : {cl[2]}, email : {cl[3]}')
                phone_client(conn, cl[0])
                print()  
        else:
            cur.execute("""
                SELECT * FROM client WHERE client_id=%s;
                """, (id,))
            cl=cur.fetchone()
            if cl==None:
                print('Клиент не найден')
            else:
                print(f'id : {cl[0]},  Имя : {cl[1]},  Фамилия : {cl[2]}, email : {cl[3]}')
                phone_client(conn, cl[0])
                print()
    return 


def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if phone!=None:
            if phone=='':
                phone=None
            elif phone.isdigit()==False:
                phone=None
        if  email=='':
            email=None
        if first_name=='':
            first_name=None
        if last_name=='':
            last_name=None
            
        if email!=None and phone!=None:
            cur.execute("""
                SELECT * FROM client WHERE email=%s;
                """, (email,))
            cl_email=cur.fetchone()
            cur.execute("""
                SELECT client_id FROM telephone WHERE phone=%s;
                """, (phone,))
            client_id=cur.fetchone()
            if cl_email[0]!=client_id:
                print('Клиент не найден')
            elif cl_email[0]!=client_id:
                find_last_client(conn, first_name, last_name, client_id)

        elif email!=None:
            cur.execute("""
                SELECT * FROM client WHERE email=%s;
                """, (email,))
            cl_email=cur.fetchone()
            if cl_email==None:
                print('Клиент не найден')
            else:
                find_last_client(conn, first_name, last_name, cl_email[0])

        elif phone!=None: 
            cur.execute("""
                SELECT client_id FROM telephone WHERE phone=%s;
                """, (phone,))
            client_id=cur.fetchone()  
            if client_id==None:
                print('Клиент не найден')
            else:
                find_last_client(conn, first_name, last_name, client_id)

        elif email==None and phone==None:
            if first_name!=None or last_name!=None:
                if first_name!=None and last_name!=None:
                    cur.execute("""
                    SELECT * FROM client WHERE first_name=%s AND last_name=%s;
                    """, (first_name, last_name, ))
                    cl=cur.fetchall()
                elif first_name!=None:
                    cur.execute("""
                    SELECT * FROM client WHERE first_name=%s;
                    """, (first_name,))
                    cl=cur.fetchall()
                elif last_name!=None:
                    cur.execute("""
                    SELECT * FROM client WHERE last_name=%s;
                    """, (last_name,))
                    cl=cur.fetchall()
                if cl==None:
                    print('Клиент не найден')
                else:
                    for i in range(len(cl)):
                        print(f'id : {cl[i][0]},  Имя : {cl[i][1]},  Фамилия : {cl[i][2]}, email : {cl[i][3]}, телефон : ', end='')
                        phone_client(conn, cl[i][0])
                        print()
            else:
                print('Клиент не найден')

    return 

def find_client2(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if phone!=None:
            if phone=='':
                phone=None
            elif phone.isdigit()==False:
                phone=None
        if  email=='':
            email=None
        if first_name=='':
            first_name=None
        if last_name=='':
            last_name=None

        h=[]
        k=0
        if email!=None:
            cur.execute("""
                SELECT client_id FROM client WHERE email=%s;
                """, (email,))
            id_email=cur.fetchone()
            h.append(id_email)
            k+=1
        if phone!=None: 
            cur.execute("""
                SELECT client_id FROM telephone WHERE phone=%s;
                """, (phone,))
            id_phone=cur.fetchone()
            h.append(id_phone)
            k+=1  
        if first_name!=None:
            cur.execute("""
                SELECT client_id FROM client WHERE first_name=%s;
                """, (first_name,))
            id_first_name=cur.fetchall()
            for i in range(len(id_first_name)):
                id=id_first_name[i][0]
                h.append(id)
            k+=1
        if last_name!=None:
            cur.execute("""
                SELECT client_id FROM client WHERE last_name=%s;
                """, (last_name,))
            id_last_name=cur.fetchall()
            for i in range(len(id_last_name)):
                id=id_last_name[i][0]
                h.append(id)
            k+=1
        
        if len(h)==0:
            print('Клиент не найден')
        else:
            cnt1 = h.count(h[0])
            if len(h)==k and cnt1==k:
                id=h[0]
                cur.execute("""
                    SELECT * FROM client WHERE client_id=%s;
                    """, (id,))
                cl=cur.fetchall()
                print(f'id : {cl[i][0]},  Имя : {cl[i][1]},  Фамилия : {cl[i][2]}, email : {cl[i][3]}, телефон : ', end='')
                phone_client(conn, cl[i][0])
                print()
            elif len(h)==k and cnt1!=k:
                print('Клиент не найден')
            elif len(h)>k:
                h2 = list(set(h))
                h3=[]
                for i in range(len(h2)):
                    if h.count(h2[i])==k:
                        cur.execute("""
                        SELECT * FROM client WHERE client_id=%s;
                        """, (h2[i],))
                        cl=cur.fetchall()
                        print(f'id : {cl[0][0]},  Имя : {cl[0][1]},  Фамилия : {cl[0][2]}, email : {cl[0][3]}, телефон : ', end='')
                        phone_client(conn, cl[0][0])
                        print()
                    else:
                        h3.append(h2[i])
                if h2==h3:
                    print('Клиент не найден')
    return
       

with psycopg2.connect(database="client", user="postgres", password="postgres") as conn:
    # with conn.cursor() as cur:
    #      cur.execute("""
    #      DROP TABLE telephone;
    #      DROP TABLE client;
    #      """)

    # create_db(conn)

    ivanov = add_client(conn, 'Иван', 'Иванов', 'ivanov@mail.ru', '111119991111/44444444444')

    petrov = add_client(conn, 'Петр', 'Петров', 'petrov@mail.ru')
      
    petrov_alexandr = add_client(conn, 'Александр', 'Петров', 'petrov_al@mail.ru', '22222222222/659857928')
    
    sergeev = add_client(conn, 'Сергей', 'Сергеев', 'sergeev@mail.ru', '33333333333')

    #client = add_client(conn, input('Введите имя: '), input('Введите фамилию: '), input('Введите email: '), input('Введите номера телефонов через / : '))

    add_phone(conn, 3, '958268')

    #client_phone=add_phone(conn, int(input('Введите id: ')), input('Введите номер телефонa: '))

    change_client(conn, 3, email='aleksandr_petrov@mail.ru')

    #client_change = change_client(conn, int(input('Введите id: '), input('Введите имя: '), input('Введите фамилию: '), input('Введите email: '), input('Введите номера телефонов через / : '))

    delete_phone(conn, 1, 44444444444)

    #client_delete_phone = delete_phone(conn, int(input('Введите id: ')), int(input('Введите номер телефонa: ')))

    delete_client(conn, 4)

    #client_delete = delete_client(conn, int(input('Введите id: ')))

    find_client(conn, first_name=None, last_name='Петров', email=None, phone=None)
    find_client(conn, first_name=None, last_name='Петров', email=None, phone='777777')
    find_client(conn, first_name='Петр', last_name=None, email=None, phone=None)
    find_client(conn, first_name=None, last_name='Иванов', email=None, phone=None)
    find_client(conn, first_name=None, last_name=None, email=None, phone=None)
     # client_find = find_client(conn, input('Введите имя: '), input('Введите фамилию: '), input('Введите email: '), input('Введите номер телефона: '))


    find_client2(conn, first_name=None, last_name='Петров', email=None, phone=None)
    find_client2(conn, first_name=None, last_name='Петров', email=None, phone='777777')
    find_client2(conn, first_name='Петр', last_name=None, email=None, phone=None)
    find_client2(conn, first_name=None, last_name='Иванов', email=None, phone=None)
    find_client2(conn, first_name=None, last_name=None, email=None, phone=None)
    # client_find = find_client2(conn, input('Введите имя: '), input('Введите фамилию: '), input('Введите email: '), input('Введите номер телефона: '))
   
   
conn.close()



   

       


