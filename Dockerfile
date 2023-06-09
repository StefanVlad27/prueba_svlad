# Usar una imagen base de Node.js
FROM node:14

# Crear un directorio de trabajo
WORKDIR /usr/src/app

# Copiar package.json y package-lock.json al directorio de trabajo
COPY package*.json ./

# Instalar las dependencias
RUN npm install

# Copiar el resto del código de la aplicación al directorio de trabajo
COPY . .

# Expone el puerto en el que se ejecutará la aplicación
EXPOSE 3000

# El comando para iniciar la aplicación
CMD [ "node", "app.js" ]

