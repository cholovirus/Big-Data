import os
import glob
import firebase_admin
from firebase_admin import credentials, db
import json

cred = credentials.Certificate("google-service.json")
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://dronihc-parte2-default-rtdb.firebaseio.com/'
})
ref = db.reference('/')
def guardar_en_txt(palabras, nombre_archivo):
    with open(nombre_archivo, 'w') as f:
        # Escribir cada palabra en una línea separada
        f.write('\n'.join(palabras))

def leer_archivos_txt(directorio_base,nombre):
    # Obtener una lista de todas las carpetas en el directorio base
    carpetas = [f for f in os.listdir(directorio_base) if os.path.isdir(os.path.join(directorio_base, f))]
    palabras=[]    
    for carpeta in carpetas:
        ruta_carpeta = os.path.join(directorio_base, carpeta)
        archivos_txt = glob.glob(os.path.join(ruta_carpeta, "*"))
        
        for archivo in archivos_txt:
            with open(archivo, 'r') as f:
                contenido = f.read()
                lineas = contenido.split('\n')
                if (nombre == "tupla"):
                    lineas = [item.replace("(", "").replace(")", "").replace("'", "").replace(","," ,") for item in lineas if item]
                else:
                    lineas = [item.replace("(", "").replace(")", "").replace("'", "").replace(", ",":") for item in lineas if item]
                lista = [palabra for palabra in lineas if palabra]
                
                palabras = palabras+lista
    guardar_en_txt(palabras,"/home/hadoop/log/"+nombre+".txt")

def leer_usuarios(archivo):
    # Diccionario para almacenar los datos
    usuarios = {}
    #count = []
    
    # Abrir el archivo y leer línea por línea
    with open(archivo, 'r') as f:
        for linea in f:
            # Eliminar espacios en blanco al inicio y al final de la línea
            linea = linea.strip()
            
            if linea:
                # Dividir la línea en usuario y cantidad
                partes = linea.split()
                
                # El usuario puede contener espacios, así que lo unimos
                usuario = ' '.join(partes[:-1])
                cantidad = int(partes[-1])
                
                # Agregar al diccionario
                usuarios[usuario] = cantidad
    
    return usuarios

datos_comando = ref.child('comando').get("cmd")
print('Valor obtenido:', datos_comando)

id = int(input())
if (id):
    directorio_base = "txt/tuples/"
    leer_archivos_txt(directorio_base,"tupla")
    comando= "hive -f tupla.hql"
    os.system(comando)
    ruta="/home/hadoop/log/tupla/000000_0"
    archivo = leer_usuarios(ruta)
    contenido = json.dumps(archivo, indent=4)
    ref.child('hive').set({'datos': contenido})
    os.system("cd /home/hadoop/txt/tuples && rm -r *")
    os.system("cd /home/hadoop")
else:
    directorio_base = "txt/count/"
    leer_archivos_txt(directorio_base,"count")
    comando= "hive -f count.hql"
    
    os.system(comando)
    ruta="/home/hadoop/log/count/000000_0"
    archivo = leer_usuarios(ruta)
    contenido = json.dumps(archivo, indent=4)
    ref.child('hive').set({'datos': contenido})
    os.system("cd /home/hadoop/txt/count && rm -r *")
    os.system("cd /home/hadoop")