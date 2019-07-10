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
package cs4347.jdbcGame.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcGame.dao.GamesPlayedDAO;
import java.util.ArrayList;
import cs4347.jdbcGame.dao.impl.GamesPlayedDAOImpl;
import cs4347.jdbcGame.entity.GamesPlayed;
import cs4347.jdbcGame.services.GamesPlayedService;
import cs4347.jdbcGame.util.DAOException;

public class GamesPlayedServiceImpl implements GamesPlayedService
{
    private DataSource dataSource;

    public GamesPlayedServiceImpl(DataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    @Override
    public GamesPlayed create(GamesPlayed gamesPlayed) throws DAOException, SQLException
    {
        if(gamesPlayed.getGameID() == null || gamesPlayed.getPlayerID() == null) {
        	throw new DAOException("gamesPlayed must have a gameID and playerID");
        }
        
        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        try {
        	connection.setAutoCommit(false);
        	GamesPlayed gp = gamesPlayedDAO.create(connection, gamesPlayed);
        	connection.commit();
        	return gp;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
            if (connection != null) {
                connection.setAutoCommit(true);
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
        
    }

    @Override
    public GamesPlayed retrieveByID(long gamePlayedID) throws DAOException, SQLException
    {

        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            GamesPlayed gp = gamesPlayedDAO.retrieveID(connection, gamePlayedID);
            connection.commit();
            return gp;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
        	if(connection != null) {
        		 connection.setAutoCommit(true);
        	}
        	if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    
    @Override
    public List<GamesPlayed> retrieveByPlayerGameID(long playerID, long gameID) throws DAOException, SQLException
    {
        
        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            List<GamesPlayed> gamesp = new ArrayList<>();
            gamesp = gamesPlayedDAO.retrieveByPlayerGameID(connection, playerID, gameID);
            connection.commit();
            return gamesp;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
        	if(connection != null) {
        		 connection.setAutoCommit(true);
        	}
        	if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
        
    }

    @Override
    public List<GamesPlayed> retrieveByGame(long gameID) throws DAOException, SQLException
    {
        
        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            List<GamesPlayed> gamesp = new ArrayList<>();
            gamesp = gamesPlayedDAO.retrieveByGame(connection, gameID);
            connection.commit();
            return gamesp;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
        	if(connection != null) {
        		 connection.setAutoCommit(true);
        	}
        	if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    
    @Override
    public List<GamesPlayed> retrieveByPlayer(long playerID) throws DAOException, SQLException
    {
        
        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        try {
            connection.setAutoCommit(false);
            List<GamesPlayed> gamesp = new ArrayList<>();
            gamesp = gamesPlayedDAO.retrieveByPlayer(connection, playerID);
            connection.commit();
            return gamesp;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
        	if(connection != null) {
        		 connection.setAutoCommit(true);
        	}
        	if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    @Override
    public int update(GamesPlayed gamesPlayed) throws DAOException, SQLException
    {
        if(gamesPlayed == null) {
        	throw new DAOException("invalid gamesPlayed object");
        }
        
        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        try {
        	connection.setAutoCommit(false);
        	int upd = gamesPlayedDAO.update(connection, gamesPlayed);
        	connection.commit();
        	return upd;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
        	if(connection != null) {
        		 connection.setAutoCommit(true);
        	}
        	if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }
    }

    
    @Override
    public int delete(long gamePlayedID) throws DAOException, SQLException
    {
        
        GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        	try {
        		connection.setAutoCommit(false);
        		int del = gamesPlayedDAO.delete(connection, gamePlayedID);
        		connection.commit();
        		return del;
        	}catch (Exception ex) {
                connection.rollback();
                throw ex;
            }
            finally {
            	if(connection != null) {
            		 connection.setAutoCommit(true);
            	}
            	if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }    
        
    }

    @Override
    public int count() throws DAOException, SQLException
    {
    	GamesPlayedDAO gamesPlayedDAO = new GamesPlayedDAOImpl();
        
        Connection connection = dataSource.getConnection();
        
        try {
        	connection.setAutoCommit(false);
        	int count = gamesPlayedDAO.count(connection);
        	connection.commit();
        	return count;
        }catch (Exception ex) {
            connection.rollback();
            throw ex;
        }
        finally {
         	if(connection != null) {
         		 connection.setAutoCommit(true);
         	}
         	if (connection != null && !connection.isClosed()) {
                 connection.close();
             }
         }
    }

}
