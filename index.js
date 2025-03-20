const express = require("express");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
require("dotenv").config(); // Load AWS credentials from .env

const app = express();
const PORT = 8080;

// AWS S3 Configuration
const s3 = new S3Client({
  region: process.env.AWS_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

// S3 Bucket and File Details
const BUCKET_NAME = process.env.S3_BUCKET_NAME; // e.g., "my-bucket"
const FOLDER_NAME = process.env.S3_FOLDER_NAME || ""; // e.g., "json-folder"
const FILE_NAME = process.env.S3_FILE_NAME; // e.g., "output.json"

// Route for health check
app.get("/", (req, res) => {
  res.send({ message: "Server is running" });
});

// Stream JSON from S3
app.get("/stream-json", async (req, res) => {
  res.setHeader("Content-Type", "application/json");

  try {
    const filePath = FOLDER_NAME ? `${FOLDER_NAME}/${FILE_NAME}` : FILE_NAME;
    console.log("Fetching from S3:", filePath);

    const command = new GetObjectCommand({
      Bucket: BUCKET_NAME,
      Key: filePath,
    });

    const { Body } = await s3.send(command);

    res.write("[\n");
    let isFirstObject = true;
    let buffer = "";

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
    console.error("Error streaming JSON from S3:", error);
    res.status(500).json({ error: "Failed to fetch data from S3" });
  }
});

// Start Express server
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
