package buffer;

import file.BlockId;
import file.FileMgr;
import file.Page;
import log.LogMgr;

/**
 * Pageをラップしたもので以下のような追加の情報を持つ
 * - Pageに対応するblockのblockId
 * - pinされている回数
 * - pageが更新されているかどうか
 * - 更新されている場合はtx番号とlsnも保持する
 */
public class Buffer {
    private FileMgr fm;
    private LogMgr lm;

    private Page contents;
    private BlockId blk = null;

    private int pins = 0;
    private int txnum = -1;
    private int lsn = -1;

    public Buffer(FileMgr fm, LogMgr lm) {
        this.fm = fm;
        this.lm = lm;
        contents = new Page(fm.blockSize());
    }

    public Page contents() {
        return contents;
    }

    public BlockId block() {
        return blk;
    }

    public void setModified(int txnum, int lsn) {
        this.txnum = txnum;
        if (lsn > 0) {
            this.lsn = lsn;
        }
    }

    public boolean isPinned() {
        return pins > 0;
    }

    public int modifyingTx() {
        return txnum;
    }

    void assignToBlock(BlockId b) {
        flush(); // modifyされた内容はディスクに永続化されるのでBlockに対応する値が変わっても安心
        blk = b; // fm.readするわけではないので、直後にcontentsからgetIntなどをすると違うブロックのデータが取れる点には注意（readしなくていいケースがあるならパフォーマンス的にしない方が良い）
    }

    void flush() {
        // txnnum == -1の場合はページは修正されてないのでやることがない
        if (txnum >= 0) {
            lm.flush(lsn); // lastSavedLSnを超えてないとflushされない（このtxnに関するログが書かれていればそれ以上は必要ないため）
            fm.write(blk, contents);
            txnum = -1;
        }
    }

    void pin() {
        pins++;
    }

    void unpin() {
        pins--;
    }
}
