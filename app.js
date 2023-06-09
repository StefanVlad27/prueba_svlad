const express = require('express');
const app = express();
const port = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.send('¡Hola, AWS App Runner!');
});

app.listen(port, () => {
  console.log(`Aplicación escuchando en http://localhost:${port}`);
});
