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
import java.util.List;

import cs4347.jdbcGame.dao.GamesPlayedDAO;
import cs4347.jdbcGame.entity.GamesPlayed;
import cs4347.jdbcGame.util.DAOException;

public class GamesPlayedDAOImpl implements GamesPlayedDAO
{
	
	private static final String insertSQL = "INSERT INTO gamesplayed (playerID, gameID, timeFinished, score) VALUES (?, ?, ?, ?);";

	private static final String selectSQL = "SELECT ID, playerID, gameID, timeFinished, score WHERE ID = ?;";
	
	private static final String selectPlayeraGameSQL = "SELECT ID, playerID, gameID, timeFinished, score WHERE playerID = ? AND gameID = ?;";
	
	private static final String selectPlayerSQL = "SELECT ID, playerID, gameID, timeFinished, score WHERE playerID = ?;";
	
	private static final String selectGameSQL = "SELECT ID, playerID, gameID, timeFinished, score WHERE gameID = ?;";
	
	private static final String updateSQL = "UPDATE gamesplayed SET ID = ?, playerID = ?, gameID = ?, timeFinished = ?, score = ? WHERE ID = ?;";
	
	private static final String deleteSQL = "DELETE FROM gamesplayed WHERE ID = ?;";
	
	private static final String countSQL = "SELECT COUNT(*) as count from gamesPlayed;";
    @Override
    public GamesPlayed create(Connection connection, GamesPlayed gamesPlayed) throws SQLException, DAOException
    {
    	if (gamesPlayed.getId() != null) {
            throw new DAOException("Trying to insert Player with NON-NULL ID");
        }

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
            ps.setLong(1, gamesPlayed.getPlayerID());
            ps.setLong(2, gamesPlayed.getGameID());
            ps.setDate(3, new java.sql.Date(gamesPlayed.getTimeFinished().getTime()));
            ps.setInt(4, gamesPlayed.getScore());
            ps.executeUpdate();

            // Copy the assigned ID to the customer instance.
            ResultSet keyRS = ps.getGeneratedKeys();
            keyRS.next();
            int lastKey = keyRS.getInt(1);
            gamesPlayed.setId((long) lastKey);
            return gamesPlayed;
        }
        finally {
            if (ps != null && !ps.isClosed()) {
                ps.close();
            }
        }
    }

    
    @Override
    public GamesPlayed retrieveID(Connection connection, Long gamePlayedID) throws SQLException, DAOException
    {
        if(gamePlayedID == null) {
        	throw new DAOException("Must provide valid ID");
        }

        PreparedStatement ps = null;
        
        try {
        	ps = connection.prepareStatement(selectSQL);
        	ps.setLong(1, gamePlayedID);
    		//execute the query and store the results in keyRS
    		ResultSet keyRS = ps.executeQuery();
    		
    		if(keyRS.next()) {
    			Long id = keyRS.getLong("ID");
    			Long playerID = keyRS.getLong("playerID");
    			Long gameID = keyRS.getLong("gameID");
    			Date timeFinished = keyRS.getDate("timeFinished");
    			int score = keyRS.getInt("score");
    			
    			//create new GamesPlayed object
    			GamesPlayed gp = new GamesPlayed();
    			gp.setId(id);
    			gp.setPlayerID(playerID);
    			gp.setGameID(gameID);
    			gp.setTimeFinished(timeFinished);
    			gp.setScore(score);
    			
    			return gp;
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
    public List<GamesPlayed> retrieveByPlayerGameID(Connection connection, Long playerID, Long gameID)
            throws SQLException, DAOException
    {
    	if(playerID == null || gameID == null) {
            throw new DAOException("Must provide playerID to pull records");
    	}
    	
    	PreparedStatement ps = null;
    	
    	try {
    		ps = connection.prepareStatement(selectPlayeraGameSQL);
    		ps.setLong(1, playerID);
    		ps.setLong(2, gameID);
    		ResultSet keyRS = ps.exectueQuery();
    		
    		List<GamesPlayed> games = new ArrayList<>();
    		
    		while(keyRS.next()) {
    			Long id = keyRS.getLong("ID");
    			Long pID = keyRS.getLong("playerID");
    			Long gID = keyRS.getLong("gameID");
    			Date timeFinished = keyRS.getDate("timeFinished");
    			int score = keyRS.getInt("score");
    			
    			GamesPlayed gp = new GamesPlayed();
    			gp.setId(id);
    			gp.setPlayerID(pID);
    			gp.setGameID(gID);
    			gp.setTimeFinished(timeFinished);
    			gp.setScore(score);
    			
    			games.add(gp);
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
    public List<GamesPlayed> retrieveByPlayer(Connection connection, Long playerID) throws SQLException, DAOException
    {
    	if(playerID == null) {
    		throw new DAOException("Invalid playerId provided");
    	}
    	
    	PreparedStatement ps = null;
    	
    	try {
    		ps = connection.prepareStatement(selectPlayerSQL);
    		ps.setLong(1, playerID);
    		ResultSet keyRS = ps.exectueQuery();
    		
    		List<GamesPlayed> games = new ArrayList<>();
    		
    		while(keyRS.next()) {
    			Long id = keyRS.getLong("ID");
    			Long pID = keyRS.getLong("playerID");
    			Long gID = keyRS.getLong("gameID");
    			Date timeFinished = keyRS.getDate("timeFinished");
    			int score = keyRS.getInt("score");
    			
    			GamesPlayed gp = new GamesPlayed();
    			gp.setId(id);
    			gp.setPlayerID(pID);
    			gp.setGameID(gID);
    			gp.setTimeFinished(timeFinished);
    			gp.setScore(score);
    			
    			games.add(gp);
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
    public List<GamesPlayed> retrieveByGame(Connection connection, Long gameID) throws SQLException, DAOException
    {
        if(gameID == null) {
        	throw new DOAException("Invalid gameID given");
        }
        
        PreparedStatement ps = null;
    	
    	try {
    		ps = connection.prepareStatement(selectGameSQL);
    		ps.setLong(1, gameID);
    		ResultSet keyRS = ps.exectueQuery();
    		
    		List<GamesPlayed> games = new ArrayList<>();
    		
    		while(keyRS.next()) {
    			Long id = keyRS.getLong("ID");
    			Long pID = keyRS.getLong("playerID");
    			Long gID = keyRS.getLong("gameID");
    			Date timeFinished = keyRS.getDate("timeFinished");
    			int score = keyRS.getInt("score");
    			
    			GamesPlayed gp = new GamesPlayed();
    			gp.setId(id);
    			gp.setPlayerID(pID);
    			gp.setGameID(gID);
    			gp.setTimeFinished(timeFinished);
    			gp.setScore(score);
    			
    			games.add(gp);
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
    public int update(Connection connection, GamesPlayed gamesPlayed) throws SQLException, DAOException
    {
        if(gamesPlayed = null) {
        	throw new DAOException("Must provide GamesPlayed to be updated");
        }
        try {
	        ps = connection.prepareStatement(updateSQL);
	
	        Long id = gamesPlayed.getId();
	        Long pID = gamesPlayed.getPlayerID();
	        Long gID = gamesPlayed.getGameID();
	        date timeFinished = gamesPlayed.getTimeFinished();
	        int score = gamesPlayed.getScore();
	        
	        ps.setLong(1, id);
	        ps.setLong(2, pID);
	        ps.setLong(3, gID);
	        ps.setDate(4, timeFinished);
	        ps.setInt(5, score);
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
    public int delete(Connection connection, Long gamePlayedID) throws SQLException, DAOException
    {
        if(gamePlayedID == null) {
        	throw new DAOException("Must have gamePlayedID in order to delete record");
        }
        
        PreparedStatement ps = null;
        try {
        	// Delete row based on given credit card ID
            ps = connection.prepareStatement(deleteSQL);
            ps.setLong(1, gamePlayedID);
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
