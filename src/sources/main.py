'''
@author: Jaime Puerta
'''
import os.path
import csv
import sys
import json
import configparser
import smtplib 
import mysql.connector
from email.message import EmailMessage 

#Funcionalidad para la lectura de archivos y cargar la información a la base de datos
def readFiles_FillDB (nombreFile, encabezadoJson):
    
    try:        
        #Obtener información del nombre del archivo
        fileName = os.path.splitext(nombreFile)
                  
        #Procesar archivo con extensión CSV
        if fileName[1] == ".csv":
            try:
                #Proceso el archivo
                with open (nombreFile) as File:
                    
                    reader = csv.DictReader (File)
                    
                    #Leo los registros y los inserto en la base de datos
                    for row in reader:
                       
                        query = "INSERT into mydb.T_Owner (user_owner,name_owner, mail_owner, mail_manager) VALUES (%s,%s,%s,%s);"
                        parametrosQuery = parametrosQuery = (row['user_id'], '', '', row['user_manager'])
                        executeQuery(query, parametrosQuery)
                        
            except:
                print ("Exception generada ", sys.exc_info()[0])
            
            finally:
                if not (File.closed):
                    File.close()
        
        # Funcionalidad para procesar los archivos JSON
        elif fileName [1] == ".json":
            try:
                #Proceso el archivo
                with open (nombreFile) as File:
                    
                    reader = json.load(File)
                    
                    #Leo los registros y los inserto en la base de datos
                    for row in reader [encabezadoJson]:
                        
                        #Si al leer la información el registro viene vacio, se carga como que no tiene información
                        confidencialityCode = 4
                        integrityCode = 4
                        availabilityCode = 4
                        
                        #Cargo la información de las variables                     
                        confidenciality = (row['classification'])['confidentiality']
                        integrity = (row['classification'])['integrity']
                        availability = (row['classification'])['availability']
                        clasification = 'No aplica'
                        
                        #Recupero los codigos asociados
                        confidencialityCode = clasificationCode (confidenciality)
                        integrityCode = clasificationCode (integrity)
                        availabilityCode = clasificationCode (availability)
                        
                        #Si alguno de los valores es high cargo el valor
                        if (confidenciality == 'high') or (integrity == 'high') or (availability == 'high'):
                            clasification = 'high'
                             
                        #Inserto la información en la base de datos consolidada   
                        query = "INSERT into mydb.T_Info_DB (bd_name,T_Owner_user_owner, confidenciality, integrity, availability, clasification) VALUES (%s,%s,%s,%s,%s,%s);"
                        parametrosQuery = parametrosQuery = (row['dn_name'], (row['owner'])['uid'], confidencialityCode, integrityCode, availabilityCode, clasification)
                        executeQuery(query, parametrosQuery)
                        
                        #Si el valor del correo no viene en la información base se asume como que no registra correo
                        mail_owner = 'No registra correo'
                        try:
                            mail_owner = (row['owner'])['email']
                            if mail_owner == '':
                                mail_owner = 'No registra correo'
                        except:
                            print ("Excepción en los valores que no trae correo, se controla el registro y queda relacionado como -No registra Correo-")
                            
                        #Actualizo la información de los correos en la base de datos
                        query = "UPDATE mydb.T_Owner set name_owner = %s, mail_owner = %s where user_owner = %s;"
                        parametrosQuery = ((row['owner'])['name'], mail_owner, (row['owner'])['uid'])
                        executeQuery(query, parametrosQuery)
                                          
            except:
                print ("Exception generada ", sys.exc_info()[0])
            
            finally:
                if not (File.closed):
                    File.close()
        else:
            print ("Formato no soportado")
    except:
        print ("Exception generada ", sys.exc_info()[0])

# Funcionalidad que me permite hacer operaciones sobre la base de datos (extraer, insertar, borrar y actualizar información
def executeQuery (query, parametrosQuery):
    
    conn = None
    try:
        #Obtengo la información de los parametros
        config = configparser.ConfigParser()
        config.read('config.ini')
       
        #Establezco la conexión a la base datos
        conn = mysql.connector.connect(host= config['DEV_DB']['DB_HOST'], database=config['DEV_DB']['DB_NAME'], user=config['DEV_DB']['DB_USER'], password=config['DEV_DB']['DB_PASS'])
        
        if conn.is_connected():
            
            # Ejecucón de los query
            if parametrosQuery is not None:
                cursor = conn.cursor(prepared=True)
                cursor.execute(query, parametrosQuery)
            else:
                cursor = conn.cursor()
                cursor.execute(query)
            
            # Obtener la información o realizar el coomit
            if query.upper().startswith ('SELECT'):
                data = cursor.fetchall()
            else:
                conn.commit()
                data = None      
        else:
            print ("No se pudo conectar a la base de datos")
    
    except mysql.connector.Error as err:
        print ("Excepción en la ejecución del Query: ", err)
        data = None
    
    finally:
            if conn is not None:
                if (conn.is_connected()):
                    cursor.close()
                    conn.close()
    return data

#Funcionalidad para el envio de correo
def getInformationToSend ():
    
    try:
        #Obtengo la información de los parametros
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        #Obtengo la información de los registros que deben ser notificados
        paramentrosQuery = ('high',)
        query = "SELECT T_Owner.user_owner, bd_name, mail_manager FROM mydb.T_Info_DB, mydb.T_Owner WHERE mydb.T_Info_DB.T_Owner_user_owner = mydb.T_Owner.user_owner and clasification = %s;"
        resultGeneral = executeQuery(query, paramentrosQuery)
        
        query = "SELECT distinct mail_manager FROM mydb.T_Info_DB, mydb.T_Owner WHERE mydb.T_Info_DB.T_Owner_user_owner = mydb.T_Owner.user_owner and clasification = %s;"
        resultSpecific = executeQuery(query, paramentrosQuery)
      
        #Agrupar las notificaciones por manager
        for row in resultSpecific:
            infoMessage = '*'
            for row_auxiliar in resultGeneral:
                if row[0].decode("utf-8") == row_auxiliar[2].decode("utf-8"):
                    if infoMessage == '*':
                        infoMessage = "* " + row_auxiliar[1].decode("utf-8")
                    else:
                        infoMessage = infoMessage + " \n* " + row_auxiliar[1].decode("utf-8")
            
            sendMail(config['GENERAL_MAIL']['SUBJECT'], row[0].decode("utf-8"), config['GENERAL_MAIL']['MESSAGE_BODY'] + "\n" + infoMessage)
            
    
    except:
        print ("Exception generada ", sys.exc_info()[0])
 
 
#Funcionalidad para enviar las notificaciones por correo electrónico
def sendMail (email_subject, receiver_email_address, messageBody):
    
    try: 
        #Obtengo la información de los paramentros de configuracion 
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        #consumir el servicio a traves de gmail
        email_smtp = "smtp.gmail.com" 
        
        # creacion del objeto del mensaje
        message = EmailMessage() 
        
        # configurar los paramentos del mensaje 
        message['Subject'] = email_subject 
        message['From'] = config['DEV_SEND_MAIL']['ORIGIN_MAIL'] 
        message['To'] = receiver_email_address 
        
        # Contenido de mensaje
        message.set_content (messageBody)
        
        # paramentros del servidor de correo 
        server = smtplib.SMTP(email_smtp, '587') 
        
        # SMTP server 
        server.ehlo() 
        
        # Conexión segura
        server.starttls() 
        
        # Autenticación a la cuenta
        server.login(config['DEV_SEND_MAIL']['USER_USER_MAIL'], config['DEV_SEND_MAIL']['PSW_MAIL']) 
        
        # Envio de correo
        server.send_message(message) 
        
    except:
        print ("Excepción en el envío del correo del alguno de los usuarios, por favor revisar:", sys.exc_info()[0])
    finally:  
        # Cierro la conexión con el servidor
        server.quit()       
 
#Funcionalidad para obtner el codigo asociado con la clasificación de la criticidad de la informacion
def clasificationCode (variable):
    if variable == 'high':
            return 1
    elif variable == 'medium':
        return 2
    elif variable == 'low':
        return 3
    else:
        return 4

#Borro la base de datos en caso que existan registros previos
print("(1) Inicia borrado de base de datos, en caso que exista información previa.")
query = "DELETE FROM mydb.T_Info_DB;"
executeQuery(query, "")  
query = "DELETE FROM mydb.T_Owner;"
executeQuery(query, "") 
#print("-Finaliza borrado de base de datos-")

#Inicio la lectra de achivos y carga en base de datos
print("(2) Iniciar proceso de lectura de archivo CSV")
readFiles_FillDB("user_manager.csv", '')
#print("- Finaliza proceso de lectura de archivo CSV-")
print("(3) Iniciar proceso de lectura de archivo JSON")
readFiles_FillDB ("dblist.json", 'db_list')
#print("-Finaliza proceso de lectura de archivo JSON-")

#Reviso la información que debe ser informada y envío los correos
print("4) Iniciar proceso envío de notificaciones")
getInformationToSend()
print("*** Finaliza la ejecución de todo el proceso ***")

