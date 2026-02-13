FROM node:20-slim

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --omit=dev
COPY . .

EXPOSE 3002

# Heap V8 ~28GB (con límite de contenedor en 32GB)
CMD ["node", "--max-old-space-size=32896", "excelAutoKNFO.js"]