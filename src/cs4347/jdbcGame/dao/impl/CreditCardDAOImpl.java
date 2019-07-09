/* NOTICE: All materials provided by this project, and materials derived 
 * from the project, are the property of the University of Texas. 
 * Project materials, or those derived from the materials, cannot be placed 
 * into publicly accessible locations on the web. Project materials cannot 
 * be shared with other project teams. Making project materials publicly 
 * accessible, or sharing with other project teams will result in the 
 * failure of the team responsible and any team that uses the shared materials. 
 * Sharing project materials or using shared materials will also result 
 * in the reporting of all team members for academic dishonesty. 
 */
package cs4347.jdbcGame.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcGame.dao.CreditCardDAO;
import cs4347.jdbcGame.entity.CreditCard;
import cs4347.jdbcGame.util.DAOException;

public class CreditCardDAOImpl implements CreditCardDAO
{
    private static final String insertSQL = "INSERT INTO creditcard(ccName, ccNumber, expDate, securityCode, playerID) "
            + "VALUES(?,?,?,?,?);";
    
    private static final String selectSQL = "SELECT ccName, ccNumber, expDate, securityCode, playerID, ID FROM creditcard WHERE ID = ?;";
    
    private static final String selectPlayerSQL = "SELECT ccName, ccNumber, expDate, securityCode, playerID, ID FROM creditcard WHERE playerID = ?;";
    
    private static final String deleteSQL = "DELETE FROM creditcard WHERE ID = ?;";
    
    private static final String deletePlayerSQL = "DELETE FROM creditcard WHERE playerID = ?;";
    
    private static final String updateSQL = "UPDATE creditcard SET ccName = ?, ccNumber = ?, expDate = ?, securityCode = ?, playerID = ? WHERE ID = ?";
    
    private static final String countSQL = "SELECT COUNT(*) as count FROM creditcard";

    @Override
    public CreditCard create(Connection connection, CreditCard creditCard, Long playerID)
            throws SQLException, DAOException
    {
    	
        if (creditCard.getId() != null) {
            throw new DAOException("Trying to insert CreditCard with NON-NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);

            ps.setString(1, creditCard.getCcName());
            ps.setString(2, creditCard.getCcNumber());
            ps.setString(3, creditCard.getExpDate());
            ps.setInt(4, creditCard.getSecurityCode());
            ps.setLong(5, playerID);
            ps.executeUpdate();
            // System.out.println(("creditCard.getCcName()," + creditCard.getCcName() + "creditCard.getCcNumber()," + creditCard.getCcNumber() + "creditCard.getExpDate()," + creditCard.getExpDate() + "creditCard.getSecurityCode()," + creditCard.getSecurityCode() + "playerID," + playerID + ""));
            // Copy the assigned ID to the game instance.
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            creditCard.setId((long) lastKey);
            creditCard.setPlayerID(playerID);
            return creditCard;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public CreditCard retrieve(Connection connection, Long ccID) throws SQLException, DAOException
    {
    	if (ccID == null) {
            throw new DAOException("Must provide ccID to pull records");
        }

        PreparedStatement ps = null;
        try {
        	// Execute SQL SELECT for given credit card ID
            ps = connection.prepareStatement(selectSQL);
            ps.setLong(1, ccID);
            ResultSet keyRS = ps.executeQuery();
            
            // Ensure that we have a record
            if ( keyRS.next() )
            {
            	// Get values from row
            	// ccName, ccNumber, expDate, securityCode, playerID
	            String ccName = keyRS.getString("ccName");
	            String ccNumber = keyRS.getString("ccNumber");
	            String expDate = keyRS.getString("expDate");
	            int securityCode = keyRS.getInt("securityCode");
	            Long playerID = keyRS.getLong("playerID");
	            Long id = keyRS.getLong("ID");
	            
	            // System.out.println("ccName=" + ccName + ",ccNumber=" + ccNumber + ",expDate=" + expDate + ",securityCode=" + securityCode + ",playerID=" + playerID + ",id=" + id + "");
	            
	            // Create new CC object with row's values
	            CreditCard creditCard = new CreditCard();
	            creditCard.setCcName(ccName);
	            creditCard.setCcNumber(ccNumber);
	            creditCard.setExpDate(expDate);
	            creditCard.setSecurityCode(securityCode);
	            creditCard.setPlayerID(playerID);
	            creditCard.setId(id);
	            
	            // return CC object
	            return creditCard;
            }
            return null;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public List<CreditCard> retrieveCreditCardsForPlayer(Connection connection, Long playerID)
            throws SQLException, DAOException
    {
    	if (playerID == null) {
            throw new DAOException("Must provide playerID to pull records");
        }

    	// Ensure that we have records
        PreparedStatement ps = null;
        try {
        	// Execute SQL SELECT for given player ID
            ps = connection.prepareStatement(selectPlayerSQL);
            ps.setLong(1, playerID);
            ResultSet keyRS = ps.executeQuery();
            
            
            // Create List object to hold our retrieved cards
            List<CreditCard> cards = new ArrayList<CreditCard>();
            
            // Ensure that we have records and loop through them
            while ( keyRS.next() )
            {
            	// Get values from row
            	String ccName = keyRS.getString("ccName");
	            String ccNumber = keyRS.getString("ccNumber");
	            String expDate = keyRS.getString("expDate");
	            int securityCode = keyRS.getInt("securityCode");
	            Long pID = keyRS.getLong("playerID");
	            Long id = keyRS.getLong("ID");
	            
	            // Create new CC object with row's values
	            CreditCard creditCard = new CreditCard();
	            creditCard.setCcName(ccName);
	            creditCard.setCcNumber(ccNumber);
	            creditCard.setExpDate(expDate);
	            creditCard.setSecurityCode(securityCode);
	            creditCard.setPlayerID(pID);
	            creditCard.setId(id);
	            
            	// push CC object to List
	            cards.add(creditCard);
            }
            // return the List
            return cards;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public int update(Connection connection, CreditCard creditCard) throws SQLException, DAOException
    {
    	if (creditCard == null) {
            throw new DAOException("Must provide CreditCard to update record");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(updateSQL);
            
            // Get values from given CreditCard Object
            String ccName = creditCard.getCcName();
            String ccNumber = creditCard.getCcNumber();
            String expDate = creditCard.getExpDate();
            int securityCode = creditCard.getSecurityCode();
            Long playerID = creditCard.getPlayerID();
            Long id = creditCard.getId();
            
            System.out.println("ccName=" + ccName + ",ccNumber=" + ccNumber + ",expDate=" + expDate + ",securityCode=" + securityCode + ",playerID=" + playerID + ",id=" + id + "");
            
            // Execute query with given Credit Card Object
            ps.setString(1, ccName);
            ps.setString(2, ccNumber);
            ps.setString(3, expDate);
            ps.setInt(4, securityCode);
            ps.setLong(5, playerID);
            ps.setLong(6, id);
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public int delete(Connection connection, Long ccID) throws SQLException, DAOException
    {
    	if (ccID == null) {
            throw new DAOException("Must have ccID in order to delete record");
        }

        PreparedStatement ps = null;
        try {
        	// Delete row based on given credit card ID
            ps = connection.prepareStatement(deleteSQL);
            ps.setLong(1, ccID);
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public int deleteForPlayer(Connection connection, Long playerID) throws SQLException, DAOException
    {
    	if (playerID == null) {
            throw new DAOException("Must have playerID in order to delete record");
        }

        PreparedStatement ps = null;
        try {
        	// Delete row(s) based on player ID
            ps = connection.prepareStatement(deletePlayerSQL);
            ps.setLong(1, playerID);
            int rows = ps.executeUpdate();
            return rows;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public int count(Connection connection) throws SQLException, DAOException
    {
        PreparedStatement ps = null;
        try {
        	// Count number of rows in CC Table
            ps = connection.prepareStatement(countSQL);
            ResultSet keyRS = ps.executeQuery();
            if ( keyRS.next() )
            {
	            int count = keyRS.getInt("count");
	            return count;
            }
            return 0;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

}
