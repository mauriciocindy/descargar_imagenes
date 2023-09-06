import fs from 'fs';
import csv from 'csv-parser';
import fetch from 'node-fetch';
import imageType from 'image-type';
import sharp from 'sharp';

// VARIABLES

const origenCSV = `no_variantes_magnus.csv`
const carpetaSalida = `no_variantes`

// Función para descargar una imagen con un retardo entre descargas
async function descargarImagenConRetardo(url, sku, retardoMs) {
  return new Promise((resolve, reject) => {
    setTimeout(async () => {
      try {
        const response = await fetch(url);

        // Verificar el estado de la respuesta
        if (!response.ok) {
          const errorMessage = `Error al descargar la imagen para SKU ${sku}: ${response.statusText}`;
          console.error(errorMessage);
          resolve({ sku, imageUrl: url, error: errorMessage }); // Omitir la descarga y procesamiento de esta imagen
          return;
        }

        const buffer = await response.arrayBuffer();

        // Detectar el formato de la imagen
        const type = imageType(Buffer.from(buffer));
        const extension = type && type.ext ? `.${type.ext}` : '.jpg'; // Extensión predeterminada: .jpg

        let fileName = `./images/${carpetaSalida}/${sku}_1${extension}`;
        let count = 2;

        // Verificar si el archivo ya existe, si es así, agregar un número ascendente al final
        while (fs.existsSync(fileName)) {
          fileName = `./images/${carpetaSalida}/${sku}_${count}${extension}`;
          count++;
        }

        // Convertir a JPG si el formato no es compatible
        if (!['jpeg', 'jpg', 'png', 'gif'].includes(type?.ext)) {
          const jpgBuffer = await sharp(Buffer.from(buffer)).toFormat('jpeg').toBuffer();
          fs.writeFileSync(fileName, jpgBuffer);
          console.log(`Imagen descargada y convertida a JPG: ${fileName}`);
        } else {
          fs.writeFileSync(fileName, Buffer.from(buffer));
          console.log(`Imagen descargada: ${fileName}`);
        }

        resolve();
      } catch (error) {
        const errorMessage = `Error al descargar la imagen para SKU ${sku}: ${error.message}`;
        console.error(errorMessage);
        resolve({ sku, imageUrl: url, error: errorMessage }); // Omitir la descarga y procesamiento de esta imagen
      }
    }, retardoMs);
  });
}

// Función para escribir los datos con errores en otro CSV
function escribirDatosDeErrorEnCSV(errores) {
  const csvFilePath = 'error_log.csv';

  // Convertir los errores en un formato de matriz para escribir en el CSV
  const datosError = errores.map(({ sku, imageUrl, error }) => {
    return { sku, image_url: imageUrl, error_message: error };
  });

  // Crear el contenido CSV
  const contenidoCSV = "sku;image_url;error_message\n" + datosError.map(e => `${e.sku};${e.image_url};${e.error_message}`).join("\n");

  // Escribir el contenido en el archivo
  fs.writeFileSync(csvFilePath, contenidoCSV, { encoding: 'utf-8' });
}

// Función principal
async function main() {
  const csvFilePath = origenCSV; // Reemplaza por la ruta de tu archivo CSV
  const maxParallelDownloads = 1; // Establece el número máximo de descargas paralelas

  const stream = fs.createReadStream(csvFilePath)
    .pipe(csv({ separator: ';' }));

  const promesasDescarga = [];
  const erroresDescarga = []; // Array para almacenar los errores

  for await (const fila of stream) {
    const sku = fila.sku;
    const imageUrl = fila['image_url'];

    const retardoEntreDescargasMs = 0; // Establece el retardo entre descargas (en milisegundos)
    const promesa = descargarImagenConRetardo(imageUrl, sku, retardoEntreDescargasMs)
      .catch((error) => {
        // Capturar el error y almacenarlo en el array
        erroresDescarga.push({ sku, imageUrl, error: error.message });
      });

    promesasDescarga.push(promesa);

    // Limitar el número de descargas paralelas
    if (promesasDescarga.length >= maxParallelDownloads) {
      await Promise.all(promesasDescarga);
      promesasDescarga.length = 0;
    }
  }

  // Esperar a que finalicen las descargas restantes
  await Promise.all(promesasDescarga);

  // Si hay errores, escribirlos en un CSV
  if (erroresDescarga.length > 0) {
    escribirDatosDeErrorEnCSV(erroresDescarga);
  }

  console.log('Proceso de descarga de imágenes finalizado.');
}

main().catch((error) => {
  console.error('Ocurrió un error en la ejecución:', error);
});
