import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class RdbFileParser {
  public static void parseRdbFileAndLoadKeysValues(String dir, String filename, Map<String, RedisValue> keyValueStore) {
    String rdbFileName = dir + "/" + filename;
    try (FileInputStream fileInputStream = new FileInputStream(rdbFileName)) {
      verifyRdbFile(fileInputStream);
      skipToByte(fileInputStream, 0xFE); // skip to db
      skipToByte(fileInputStream, 0xFB); // skip to table size info
      getNextByte(fileInputStream); // skip hash size info
      getNextByte(fileInputStream); // skip hash size info
      while (true) {
        int zeroesOrEndOfFile = getNextByte(fileInputStream); // type/encoding flag, or EOF flag
        if (zeroesOrEndOfFile == 0xFF) { break; }
        // assume no expire time info ie FC or FD, 00 for value type=string
        int keySize = getSizeEncoding(fileInputStream);
        String key = asciiToString(getNBytesAscii(fileInputStream, keySize));
        int valueSize = getSizeEncoding(fileInputStream);
        String value = asciiToString(getNBytesAscii(fileInputStream, valueSize));
        keyValueStore.put(key, new RedisValue(value));
        System.out.println("Loading key: " + key + ", value: " + value + " from RDB file");
      }

      // TODO:
      // verify header
      // read metadata
      // read and process database including other data formats
      // verify end of file, checksum

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
}
