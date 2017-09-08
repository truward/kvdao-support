package com.truward.kvdao.exception;

import org.springframework.dao.DataAccessException;

/**
 * An exception thrown when cursor is no longer valid or stale.
 *
 * @author Alexander Shabanov
 */
public class InvalidCursorException extends DataAccessException {

  public InvalidCursorException(String cursor) {
    super("Cursor=" + cursor + " is stale or not valid");
  }
}
