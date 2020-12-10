const fs = require("fs");
const path = require("path");
const jsonfile = require("jsonfile");
const csv = require("fast-csv");
const gunzip = require("gunzip-file");

const srcFolder = path.join(__dirname, "src");
const tmpFolder = path.join(__dirname, "tmp");
const distFolder = path.join(__dirname, "dist");

const convertGzip = (fileName) => {
  if (!fs.existsSync(tmpFolder)) {
    fs.mkdirSync(tmpFolder);
  }

  return new Promise((resolve) => {
    const targetFile = fileName.replace(".gz", "");

    gunzip(
      path.join(srcFolder, fileName),
      path.join(tmpFolder, targetFile),
      () => {
        resolve(targetFile);
      }
    );
  });
};

const getData = (file) => {
  return fs.readFileSync(file, "utf-8");
};

const saveData = (file, data) => {
  return fs.writeFileSync(file, data);
};

(async function () {
  const srcFiles = fs.readdirSync(srcFolder);

  const jsonFileNames = await Promise.all(srcFiles.map((f) => convertGzip(f)));

  jsonFileNames.forEach((file) => {
    const data = getData(path.join(tmpFolder, file));

    saveData(
      path.join(tmpFolder, file),
      `[${data.trim().split("\n").join(",\n")}]`
    );
  });

  const total = [].concat(
    ...jsonFileNames.map((file) => {
      const data = jsonfile.readFileSync(path.join(tmpFolder, file));

      // Map the data to something nice here
      return data.map(({ Item }) => ({
        phoneNumber: Item.formattedPhoneNumber
          ? Item.formattedPhoneNumber.S
          : false,
      }));
    })
  );

  const csvStream = csv.format({ headers: true });
  var writeStream = fs.createWriteStream(path.join(distFolder, "output.csv"));
  csvStream.pipe(writeStream);

  total.forEach((row) => {
    csvStream.write(row);
  });
  csvStream.end();

  // jsonfile.writeFileSync(path.join(distFolder, 'output.json'), total);
})();
