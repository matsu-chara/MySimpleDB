package file;

/** ブロックを特定するためのID ブロック自体のサイズはOS依存 */
public record BlockId(String filename, int blknum) {}
