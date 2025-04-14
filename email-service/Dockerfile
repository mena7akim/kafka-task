FROM node:23-slim	

# Create app directory
WORKDIR /app

# Install app dependencies
COPY package*.json ./
RUN npm install

# Copy source files
COPY . .

# Run app
CMD ["npm", "run", "dev"]
