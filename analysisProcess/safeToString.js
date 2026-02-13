// Convierte de forma segura a string, evitando undefined o null
function safeToString(value) {
    return (value === undefined || value === null) ? '' : String(value);
  }
  
  module.exports = safeToString;