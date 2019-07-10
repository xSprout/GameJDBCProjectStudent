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
import java.util.Date;
import java.util.List;

import cs4347.jdbcGame.dao.PlayerDAO;
import cs4347.jdbcGame.entity.Player;
import cs4347.jdbcGame.util.DAOException;

public class PlayerDAOImpl implements PlayerDAO
{

    private static final String insertSQL = "INSERT INTO player (firstName, lastName, join_Date, email) VALUES (?, ?, ?, ?);";
    
    private static final String selectSQL = "SELECT firstName, lastName, join_Date, email, ID, FROM player WHERE ID = ?;";
    
    private static final String updateSQL = "UPDATE player SET firstName = ?, lastName = ?, join_Date = ?, email = ? WHERE ID = ?;";
    
    private static final String deleteSQL = "DELETE FROM player WHERE ID = ?;";
    
    private static final String countSQL = "SELECT COUNT(*) as count FROM player;";
    
    private static final String joinDateSQL = "SELECT firstName, lastName, join_Date, email FROM player WHERE join_Date BETWEEN ? and ?;";

    @Override
    public Player create(Connection connection, Player player) throws SQLException, DAOException
    {
        if (player.getId() != null) {
            throw new DAOException("Trying to insert Player with NON-NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, player.getFirstName());
            ps.setString(2, player.getLastName());
            ps.setDate(3, new java.sql.Date(player.getJoinDate().getTime()));
            ps.setString(4, player.getEmail());
            ps.executeUpdate();

            // Copy the assigned ID to the customer instance.
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            player.setId((long) lastKey);
            return player;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    @Override
    public Player retrieve(Connection connection, Long playerID) throws SQLException, DAOException
    {
    	if(playerID == null) {
    		throw new DAOException("Must provide valid playerID to retrieve");
    	}
    	
    	PreparedStatement ps = null;
    	
    	try {
    		//try to select player from playerID
    		ps = connection.prepareStatement(selectSQL);
    		//set ? equal to the player id in the selct statement
    		ps.setLong(1, playerID);
    		//execute the query and store the results in keyRS
    		ResultSet keyRS = ps.executeQuery();
    		
    		//ensure that we have a result
    		if(keyRS.next()) {
    			//get values and set them equal to variables
    			//firstName, lastName, joinDate, email and ID
    			String firstName = keyRS.getString("firstName");
    			String lastName = keyRS.getString("lastName");
    			Date joinDate = keyRS.getDate("join_Date");
    			String email = keyRS.getString("email");
    			Long id = keyRS.getLong("ID");
    			
    			//create new player object with the assigned values
    			Player player = new Player();
    			player.setFirstName(firstName);
    			player.setLastName(lastName);
    			player.setJoinDate(joinDate);
    			player.setEmail(email);
    			player.setId(id);
    			
    			//return the newly created object
    			return player;
    		}
    		return null;
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    		
    		
    	}
       
    }

    
    @Override
    public int update(Connection connection, Player player) throws SQLException, DAOException
    {
    	if(player == null) {
    		throw new DAOException("Must provide valid player to update");
    	}
    	
    	PreparedStatement ps = null;
    	
    	try {
    		ps = connection.prepareStatement(updateSQL);
    		
    		//get and assign value
    		String firstName = player.getFirstName();
    		String lastName = player.getLastName();
    		Date joinDate = player.getJoinDate();
    		String email = player.getEmail();
    		Long id = player.getId();
    		
    		ps.setString(1, firstName);
    		ps.setString(2, lastName);
    		ps.setDate(3, joinDate);
    		ps.setString(4, email);
    		ps.setLong(5, id);
    		int rows = ps.executeUpdate();
    		return rows;
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    }

    
    @Override
    public int delete(Connection connection, Long playerID) throws SQLException, DAOException
    {
        if(playerID == null) {
        	throw new DAOException("Must provide valid playerID to delete");
        }
        
        PreparedStatement ps = null;
        
        try {
        	//delete row based on provided palyer id
        	ps = connection.prepareStatement(deleteSQL);
        	ps.setLong(1, playerID);
        	//get the number of rows deleted
        	int rows = ps.executeUpdate();
        	return rows;
        }
        finally {
        	if(ps != null && !ps.isclosed()){
        		ps.close();
        	}
        }
    }

    @Override
    public int count(Connection connection) throws SQLException, DAOException
    {
    	PreparedStatement ps = null;
    	try {
    		//count the number of rows in the player table
    		ps = connection.prepareStatement(countSQL);
    		ResultSet keyRS = ps.executeQuery();
    		
    		if(keyRS.next()) {
    			int count = keyRS.getInt("count");
    			return count;
    		}
    		return 0;
    	}
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    }

    
    @Override
    public List<Player> retrieveByJoinDate(Connection connection, Date start, Date end)
            throws SQLException, DAOException
    {
    	if(start == null || end == null) {
    		throw new DAOException("An invalid date has been given");
    	}
    	
    	PreparedStatement ps = null;
    	
    	try {
    		//ready up the sql statement in ps
    		ps = connection.prepareStatement(joinDateSQL);
    		//set the start date and end date
    		ps.setDate(1, start);
    		ps.setDate(2, end);
    		
    		//execute the statement
    		ResultSet keyRS = ps.executeQuery();
    		
    		//create a list of players to return 
    		List<Player> playerList = new ArrayList<>();
    		
    		//create a loop to insert the players into the list
    		while(keyRS.next()) {
    			//get and store the values from a retrieved object
    			String firstName = keyRS.getString("firstName");
    			String lastName = keyRS.getString("lastName");
    			Date joinDate = keyRS.getDate("join_Date");
    			String email = keyRS.getString("email");
    			Long id = keyRS.getLong("ID");
    			
    			//create a new player to put in the list
    			//put the new variables created above in the player object
    			Player player = new Player();
    			player.setFirstName(firstName);
    			player.setLastName(lastName);
    			player.setJoinDate(joinDate);
    			player.setEmail(email);
    			player.setId(id);
    			
    			//put the new player object on the array list
    			playerList.add(player);
    		}
    		return palyerList;	
    	}
    	//close ps
    	finally {
    		if(ps != null && !ps.isClosed()) {
    			ps.close();
    		}
    	}
    	
    }

}
