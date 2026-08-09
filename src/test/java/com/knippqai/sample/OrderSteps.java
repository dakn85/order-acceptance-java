package com.knippqai.sample;

import com.thoughtworks.gauge.Step;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderSteps {
    private int ordered;
    private int executed;

    @Step("An order for <quantity> units is opened")
    public void openOrder(int quantity) {
        ordered = quantity;
        executed = 0;
    }

    @Step("<quantity> units are executed")
    public void execute(int quantity) {
        executed += quantity;
    }

    @Step("The remaining quantity is <quantity>")
    public void remainingIs(int quantity) {
        assertEquals(quantity, ordered - executed);
    }
}
