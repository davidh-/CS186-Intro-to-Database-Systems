package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.io.Page;
import edu.berkeley.cs186.database.table.RecordID;

import java.util.Collections;
import java.util.List;

/**
 * An inner node of a B+ tree. An InnerNode header contains an `isLeaf` flag
 * set to 0 and the page number of the first child node (or -1 if no child
 * exists). An InnerNode contains InnerEntries.
 *
 * Inherits all the properties of a BPlusNode.
 */
public class InnerNode extends BPlusNode {
    public static int headerSize = 5;       // isLeaf + pageNum of first child

    public InnerNode(BPlusTree tree) {
        super(tree, false);
        tree.incrementNumNodes();
        getPage().writeByte(0, (byte) 0);   // isLeaf = 0
        setFirstChild(-1);
    }

    public InnerNode(BPlusTree tree, int pageNum) {
        super(tree, pageNum, false);
        if (getPage().readByte(0) != (byte) 0) {
            throw new BPlusTreeException("Page is not Inner Node!");
        }
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    public int getFirstChild() {
        return getPage().readInt(1);
    }

    public void setFirstChild(int val) {
        getPage().writeInt(1, val);
    }

    /**
     * Finds the correct child of this InnerNode whose subtree contains the
     * given key.
     *
     * @param key the given key
     * @return page number of the child of this InnerNode whose subtree
     * contains the given key
     */
    public int findChildFromKey(DataBox key) {
        int keyPage = getFirstChild();  // Default keyPage
        List<BEntry> entries = getAllValidEntries();
        for (BEntry ent : entries) {
            if (key.compareTo(ent.getKey()) < 0) {
                break;
            }
            keyPage = ent.getPageNum();
        }
        return keyPage;
    }

    /**
     * Inserts a LeafEntry into the corresponding LeafNode in this subtree.
     *
     * @param ent the LeafEntry to be inserted
     * @return the InnerEntry to be pushed/copied up to this InnerNode's parent
     * as a result of this InnerNode being split, null otherwise
     */
    public InnerEntry insertBEntry(LeafEntry ent) {
        // Implement me!
        List<BEntry> entries = this.getAllValidEntries();
        BEntry result = null;
        for(BEntry e : entries) {
            if (ent.compareTo(e) < 0) {
                BPlusNode node = getBPlusNode(this.getTree(), findChildFromKey(e.getKey()));
                result = node.insertBEntry(ent);
                break;
            }
        }
        if (this.hasSpace()) {
            entries.add(result);
            this.overwriteBNodeEntries(entries);
            return null;
        } else {
            return this.splitNode(ent);
        }
    }

    /**
     * Splits this InnerNode and returns the resulting InnerEntry to be
     * pushed/copied up to this InnerNode's parent as a result of the split.
     * The left node should contain d entries and the right node should contain
     * d entries.
     *
     * @param newEntry the BEntry that is being added to this InnerNode
     * @return the resulting InnerEntry to be pushed/copied up to this
     * InnerNode's parent as a result of this InnerNode being split
     */
    @Override
    public InnerEntry splitNode(BEntry newEntry) {
        // Implement me!
        List<BEntry> validEntries = getAllValidEntries();


        int d = this.numEntries;
        List<BEntry> left = validEntries.subList(0, d/2);
        List<BEntry> right = validEntries.subList((d/2)+1, d-1);

        InnerNode newNode = new InnerNode(this.getTree());
        newNode.overwriteBNodeEntries(right);

        BEntry removed = validEntries.remove((d/2)-1);
        validEntries.add(newEntry);
        Collections.sort(validEntries);
        this.overwriteBNodeEntries(left);

        return (InnerEntry) removed;
    }
}
