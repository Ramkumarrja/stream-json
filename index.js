const express = require("express");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");

const app = express();
const PORT = 3000;

const s3 = new S3Client({ region: "us-east-1" });
const BUCKET_NAME = "json-stream";
const FILE_KEY = "output.json";

app.get("/", async(req, res) => {
    res.send({message: "hi from the cloud run"})
})

app.get("/stream-json", async (req, res) => {
  res.setHeader("Content-Type", "application/json");
  console.log("enterd the stream funtion")
  try {
    const command = new GetObjectCommand({ Bucket: BUCKET_NAME, Key: FILE_KEY });
    const { Body } = await s3.send(command);

    let isFirstObject = true;
    let buffer = "";
    res.write("[\n");

    for await (const chunk of Body) {
      buffer += chunk.toString();

      let startIdx = buffer.indexOf("{");
      let endIdx = buffer.indexOf("}", startIdx);

      while (startIdx !== -1 && endIdx !== -1) {
        let jsonObj = buffer.slice(startIdx, endIdx + 1);
        buffer = buffer.slice(endIdx + 1);

        try {
          JSON.parse(jsonObj);
          if (!isFirstObject) res.write(",\n");
          res.write(jsonObj);
          isFirstObject = false;
        } catch (e) {}
      }

      if (buffer.length > 1000) {
        buffer = buffer.slice(-1000);
      }
    }

    res.write("\n]");
    res.end();
  } catch (error) {
    console.error("Error streaming JSON:", error);
    res.status(500).json({ error: "Failed to fetch data" });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});