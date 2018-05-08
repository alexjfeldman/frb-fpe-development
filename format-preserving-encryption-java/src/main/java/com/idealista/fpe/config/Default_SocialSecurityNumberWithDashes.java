package com.idealista.fpe.config;

import com.idealista.fpe.config.basic.SocialSecurityNumberWithDashes;
import com.idealista.fpe.transformer.IntToTextTransformer;
import com.idealista.fpe.transformer.TextToIntTransformer;

import java.io.Serializable;


public class Default_SocialSecurityNumberWithDashes implements Serializable {

    private Default_SocialSecurityNumberWithDashes(){}

    public static final SocialSecurityNumberWithDashes ALPHABET = new SocialSecurityNumberWithDashes();
    private static final TextToIntTransformer TEXT_TO_INT_TRANSFORMER = new GenericTransformations(ALPHABET.availableCharacters());
    private static final IntToTextTransformer INT_TO_TEXT_TRANSFORMER = new GenericTransformations(ALPHABET.availableCharacters());
    public static final Domain DOMAIN = new GenericDomain(ALPHABET, TEXT_TO_INT_TRANSFORMER, INT_TO_TEXT_TRANSFORMER);
    public static final Integer DEFAULT_MAX_LENGTH = Integer.MAX_VALUE;
    public static final Integer DEFAULT_MIN_LENGTH = 2;
    public static final LengthRange LENGTH_RANGE = new LengthRange(DEFAULT_MIN_LENGTH, DEFAULT_MAX_LENGTH);
}