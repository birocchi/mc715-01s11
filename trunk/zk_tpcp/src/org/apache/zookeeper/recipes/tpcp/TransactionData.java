package org.apache.zookeeper.recipes.tpcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

/**
 * Wrapper to transaction data
 *
 */
class TransactionData implements Serializable {
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 1L;

    private Serializable query;
    private String coordinatorID;
    private List<String> participants;

    // TODO convert participants list to long instead of string to save space

    public TransactionData(Serializable query, String coordinatorID, List<String> participants) {
        this.query = query;
        this.coordinatorID = coordinatorID;
        this.participants = participants;
    }

    /**
     * Get query data.
     * @return
     */
    public Serializable getQuery() {
        return query;
    }

    /**
     * Get coordinator's ID
     * @return
     */
    public String getCoordinatorID() {
        return coordinatorID;
    }

    /**
     * Get participant list
     * @return
     */
    public List<String> getParticipants() {
        return participants;
    }

    /**
     * Convert this object into a binary array
     * @return
     */
    public byte[] toByteArray() {
        byte[] array = null;

        try {
            java.io.ByteArrayOutputStream binaryStream = new ByteArrayOutputStream();
            ObjectOutputStream s = new ObjectOutputStream(binaryStream);
            s.writeObject(this);
            s.flush();
            array = binaryStream.toByteArray();
            binaryStream.close();
        }
        catch (IOException e) {
            // TODO handle this
        }

        return array;
    }

    /**
     * Convert a binary array into an transaction data object
     * @param array
     * @return
     */
    public static TransactionData readByteArray(byte[] array) {
        TransactionData data = null;

        try {
            java.io.ByteArrayInputStream binaryStream = new java.io.ByteArrayInputStream(array);
            ObjectInputStream s = new ObjectInputStream(binaryStream);
            data = (TransactionData)s.readObject();
            s.close();
            binaryStream.close();
        }
        catch (IOException e) {
            // TODO handle this
        }
        catch (ClassNotFoundException e) {
            // TODO handle this
        }

        return data;
    }
}
