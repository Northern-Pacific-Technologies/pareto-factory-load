package com.norpactech.pf.loader.exception;
/**
 * © 2025 Northern Pacific Technologies, LLC. All Rights Reserved. 
 *  
 * For license details, see the LICENSE file in this project root.
 */
public class EmptyRequestFileException extends Exception {

  private static final long serialVersionUID = -9020136419406112894L;

  public EmptyRequestFileException() {
    super("Empty or null Request File");
  }
}
