import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class RdbFileParser {
  public static void parseRdbFileAndLoadKeysValues(String dir, String filename, Map<String, RedisValue> keyValueStore) {
    String rdbFileName = dir + "/" + filename;
    try (FileInputStream fileInputStream = new FileInputStream(rdbFileName)) {
      verifyRdbFile(fileInputStream); // TODO: verify rest of header
      skipToByte(fileInputStream, 0xFE); // skip to db // TODO: process metadata
      skipToByte(fileInputStream, 0xFB); // skip to table size info
      getNextByte(fileInputStream); // skip hash size info TODO: process hash size
      getNextByte(fileInputStream); // skip hash size info TODO: process hash size
      while (true) {
        int valueTypeOrFlag = getNextByte(fileInputStream); // TODO: handle other type/encodings than 00 (string)
        if (valueTypeOrFlag == 0xFF) { break; } // TODO verify end of file checksum
        Long expiryTime = null;
        if (valueTypeOrFlag == 0xFC) {
          expiryTime = getNByteUnsignedInt(fileInputStream, 8);
          valueTypeOrFlag = getNextByte(fileInputStream);
        } else if (valueTypeOrFlag == 0xFD) {
          expiryTime = getNByteUnsignedInt(fileInputStream, 4);
          expiryTime *= 1_000;
          valueTypeOrFlag = getNextByte(fileInputStream);
        }
        int keySize = getSizeEncoding(fileInputStream);
        String key = asciiToString(getNBytesAscii(fileInputStream, keySize));
        int valueSize = getSizeEncoding(fileInputStream);
        String value = asciiToString(getNBytesAscii(fileInputStream, valueSize));
        if (expiryTime != null && expiryTime < System.currentTimeMillis()) {
          System.out.println("Skipping key: " + key + ", value: " + value + "; expiryTime " + expiryTime + " is in the past");
          continue;
        }
        keyValueStore.put(key, new RedisValue(value, expiryTime));
        System.out.println("Loading key: " + key + ", value: " + value + ", expiryTime: " + expiryTime + " from RDB file");
      }
    } catch (FileNotFoundException e) {
      System.out.println("File not found: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("Error reading file: " + e.getMessage());
    }
  }

  private static void verifyRdbFile(FileInputStream fileInputStream) throws RdbException {
    if (!asciiToString(getNBytesAscii(fileInputStream, 5)).equals("REDIS")) {
      throw new RdbException("Invalid RDB file header");
    }

    String rdbVersion = asciiToString(getNBytesAscii(fileInputStream, 4));
    System.out.println("Loading RDB file, version: " + rdbVersion);
  }

  private static String asciiToString(byte[] bytes) {
    return new String(bytes);
  }

  private static int getNextByte(FileInputStream fileInputStream) throws RdbException {
    try {
      int chomp = fileInputStream.read();
      if (chomp == -1) {
        throw new RdbException("Unexpected end of file");
      }
      return chomp;
    } catch (IOException e) {
      throw new RdbException("Error reading file", e);
    }
  }

  private static byte[] getNBytesAscii(FileInputStream fileInputStream, int length) throws RdbException {
    byte[] bytes = new byte[length];
    try {
      int bytesRead = fileInputStream.read(bytes);
      if (bytesRead != length) {
        throw new RdbException("Unexpected end of file");
      }
      return bytes;
    } catch (IOException e) {
      throw new RdbException("Error reading file", e);
    }
  }

  private static void skipToByte(FileInputStream fileInputStream, int targetByte) throws RdbException {
    byte b;
    System.out.println("Skipping to section by byte " + targetByte);
    do { } while (getNextByte(fileInputStream) != targetByte);
  }

  private static int getSizeEncoding(FileInputStream fileInputStream) throws RdbException {
    int firstByte = getNextByte(fileInputStream);
    System.out.println("First byte for size encoding: " + firstByte);
    // Check for encoding type and return size based on type
    int encoding = firstByte & 0xC0;
    switch (encoding) {
      case 0x00: // If the first two bits are 0b00: The size is the remaining 6 bits of the byte.
        return firstByte & 0x3F;
      case 0x40: // If the first two bits are 0b01: The size is the next 14 bits
        return (firstByte & 0x3F) << 8 | getNextByte(fileInputStream);
      case 0x80: // If the first two bits are 0b10: The size is the next 4 bytes, in big-endian (read left-to-right).
        return getNextByte(fileInputStream) << 8 | getNextByte(fileInputStream);
      case 0xC0: // If the first two bits are 0b11: The remaining 6 bits specify a type of string encoding.
        // TODO handle additional string encodings
        return getNextByte(fileInputStream) << 8 | getNextByte(fileInputStream);
      default:
        throw new RdbException("Invalid size encoding");
    }
  }

  private static long getNByteUnsignedInt(FileInputStream fileInputStream, int numBytes) throws RdbException {
    int[] bytes = new int[numBytes];
    for (int i = 0; i < numBytes; i++) {
      bytes[i] = getNextByte(fileInputStream);
    }
    long retVal = 0;
    for (int i = numBytes - 1; i >= 0; i--) {
      retVal = (retVal << 8) | bytes[i];
    }
    //System.out.println("Read unsigned int of " + numBytes + " bytes: " + retVal);
    return retVal;
  }
}
